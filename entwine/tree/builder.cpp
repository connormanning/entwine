/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/builder.hpp>

#include <limits>

#include <pdal/Dimension.hpp>
#include <pdal/PointView.hpp>

#include <entwine/compression/util.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-table.hpp>
#include <entwine/types/single-point-table.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/executor.hpp>

using namespace arbiter;

namespace entwine
{

namespace
{
    std::size_t sleepCount(65536 * 24);
    const double workToClipRatio(0.47);

    std::size_t getWorkThreads(const std::size_t total)
    {
        std::size_t num(
                std::round(static_cast<double>(total) * workToClipRatio));
        return std::max<std::size_t>(num, 1);
    }

    std::size_t getClipThreads(const std::size_t total)
    {
        return std::max<std::size_t>(total - getWorkThreads(total), 4);
    }
}

Builder::Builder(
        std::unique_ptr<Manifest> manifest,
        const std::string outPath,
        const std::string tmpPath,
        const bool compress,
        const bool trustHeaders,
        const Reprojection* reprojection,
        const BBox* bbox,
        const DimList& dimList,
        const std::size_t totalThreads,
        const Structure& structure,
        std::shared_ptr<Arbiter> arbiter)
    : m_bbox(bbox ? new BBox(*bbox) : 0)
    , m_subBBox(
            structure.subset() ?
                new BBox(structure.subset()->bbox()) : nullptr)
    , m_schema(new Schema(dimList))
    , m_structure(new Structure(structure))
    , m_reprojection(reprojection ? new Reprojection(*reprojection) : 0)
    , m_manifest(std::move(manifest))
    , m_mutex()
    , m_compress(compress)
    , m_trustHeaders(trustHeaders)
    , m_isContinuation(false)
    , m_srs()
    , m_pool(new Pool(getWorkThreads(totalThreads)))
    , m_executor(new Executor(m_structure->is3d()))
    , m_originId(m_schema->pdalLayout().findDim("Origin"))
    , m_origin(0)
    , m_end(0)
    , m_arbiter(arbiter ? arbiter : std::shared_ptr<Arbiter>(new Arbiter()))
    , m_outEndpoint(new Endpoint(m_arbiter->getEndpoint(outPath)))
    , m_tmpEndpoint(new Endpoint(m_arbiter->getEndpoint(tmpPath)))
    , m_pointPool(new Pools(m_schema->pointSize()))
    , m_registry(
            new Registry(
                *m_outEndpoint,
                *m_schema,
                *m_bbox,
                *m_structure,
                *m_pointPool,
                getClipThreads(totalThreads)))
{
    prep();
}

Builder::Builder(
        std::unique_ptr<Manifest> manifest,
        const std::string outPath,
        const std::string tmpPath,
        const std::size_t totalThreads,
        std::shared_ptr<Arbiter> arbiter)
    : m_bbox()
    , m_subBBox()
    , m_schema()
    , m_structure()
    , m_reprojection()
    , m_manifest()
    , m_mutex()
    , m_compress(true)
    , m_trustHeaders(false)
    , m_isContinuation(true)
    , m_srs()
    , m_pool(new Pool(getWorkThreads(totalThreads)))
    , m_executor()
    , m_originId()
    , m_origin(0)
    , m_end(0)
    , m_arbiter(arbiter ? arbiter : std::shared_ptr<Arbiter>(new Arbiter()))
    , m_outEndpoint(new Endpoint(m_arbiter->getEndpoint(outPath)))
    , m_tmpEndpoint(new Endpoint(m_arbiter->getEndpoint(tmpPath)))
    , m_pointPool()
    , m_registry()
{
    prep();
    load(getClipThreads(totalThreads));
}

Builder::Builder(const std::string path, std::shared_ptr<Arbiter> arbiter)
    : m_bbox()
    , m_subBBox()
    , m_schema()
    , m_structure()
    , m_reprojection()
    , m_manifest()
    , m_mutex()
    , m_compress(true)
    , m_trustHeaders(true)
    , m_isContinuation(true)
    , m_srs()
    , m_pool()
    , m_executor()
    , m_originId()
    , m_origin(0)
    , m_end(0)
    , m_arbiter(arbiter ? arbiter : std::shared_ptr<Arbiter>(new Arbiter()))
    , m_outEndpoint(new Endpoint(m_arbiter->getEndpoint(path)))
    , m_tmpEndpoint()
    , m_pointPool()
    , m_registry()
{ }

Builder::~Builder()
{ }

void Builder::go(std::size_t max)
{
    m_end = max ? std::min(max, m_manifest->size()) : m_manifest->size();

    if (m_srs.empty() && m_manifest->size()) init();
    std::size_t added(0);

    while (keepGoing())
    {
        FileInfo& info(m_manifest->get(m_origin));
        if (info.status() != FileInfo::Status::Outstanding)
        {
            next();
            continue;
        }

        const std::string path(info.path());

        if (!m_executor->good(path))
        {
            m_manifest->set(m_origin, FileInfo::Status::Omitted);
            next();
            continue;
        }

        ++added;
        std::cout << "Adding " << m_origin << " - " << path << std::endl;
        const auto origin(m_origin);

        m_pool->add([this, origin, &info, path]()
        {
            FileInfo::Status status(FileInfo::Status::Inserted);

            try
            {
                insertPath(origin, info);
            }
            catch (std::runtime_error e)
            {
                std::cout << "During " << path << ": " << e.what() << std::endl;
                status = FileInfo::Status::Error;
            }
            catch (...)
            {
                std::cout << "Unknown error during " << path << std::endl;
                status = FileInfo::Status::Error;
            }

            m_manifest->set(origin, status);
            std::cout << "\tChunks: " << Chunk::getChunkCnt() << std::endl;
        });

        next();
    }

    m_pool->join();
    save();
}

bool Builder::checkPath(
        const std::string& localPath,
        const Origin origin,
        const FileInfo& info)
{
    auto check([this](Origin origin, const BBox& bbox, std::size_t numPoints)
    {
        if (!bbox.overlaps(*m_bbox))
        {
            m_manifest->add(origin, numPoints, m_structure->primary());
            return false;
        }
        else if (m_subBBox && !bbox.overlaps(*m_subBBox))
        {
            return false;
        }

        return true;
    });

    if (const BBox* bbox = info.bbox())
    {
        return check(origin, *bbox, info.numPoints());
    }
    else if (m_trustHeaders)
    {
        auto pre(m_executor->preview(localPath, m_reprojection.get()));
        if (pre) return check(origin, pre->bbox, pre->numPoints);
    }

    // Couldn't validate anything - so we'll need to insert point-by-point.
    return true;
}

bool Builder::insertPath(const Origin origin, FileInfo& info)
{
    auto localHandle(m_arbiter->getLocalHandle(info.path(), *m_tmpEndpoint));
    const std::string& localPath(localHandle->localPath());

    if (!checkPath(localPath, origin, info)) return false;

    std::unique_ptr<Clipper> clipper(new Clipper(*this));
    SimplePointTable table(m_pointPool->dataPool(), *m_schema);

    std::size_t num(0);
    auto inserter([this, &table, origin, &clipper, &num](pdal::PointView& view)
    {
        num += view.size();
        insertView(view, table, origin, clipper.get());

        if (num >= sleepCount)
        {
            num = 0;
            clipper.reset(new Clipper(*this));
        }
    });

    return m_executor->run(table, localPath, m_reprojection.get(), inserter);
}

void Builder::insertView(
        pdal::PointView& pointView,
        SimplePointTable& table,
        const Origin origin,
        Clipper* clipper)
{
    PointStats pointStats;
    InfoPool& infoPool(m_pointPool->infoPool());

    PooledDataStack dataStack(table.stack());
    PooledInfoStack infoStack(infoPool.acquire(dataStack.size()));

    SinglePointTable localTable(*m_schema);
    LinkingPointView localView(localTable);

    PooledInfoStack rejected(infoPool);

    while (!infoStack.empty())
    {
        PooledDataNode data(dataStack.popOne());
        PooledInfoNode info(infoStack.popOne());

        localTable.setData(**data);
        localView.setField(m_originId, 0, origin);

        info->construct(
                Point(
                    localView.getFieldAs<double>(pdal::Dimension::Id::X, 0),
                    localView.getFieldAs<double>(pdal::Dimension::Id::Y, 0),
                    localView.getFieldAs<double>(pdal::Dimension::Id::Z, 0)),
                std::move(data));

        const Point& point(info->val().point());

        if (m_bbox->contains(point))
        {
            if (!m_subBBox || m_subBBox->contains(point))
            {
                Climber climber(*m_bbox, *m_structure);

                if (m_registry->addPoint(info, climber, clipper))
                {
                    pointStats.addInsert();
                }
                else
                {
                    rejected.push(std::move(info));
                    pointStats.addOverflow();
                }
            }
            else
            {
                rejected.push(std::move(info));
            }
        }
        else
        {
            rejected.push(std::move(info));
            pointStats.addOutOfBounds();
        }
    }

    m_manifest->add(origin, pointStats);
}

void Builder::load(const std::size_t clipThreads)
{
    Json::Value meta;

    {
        Json::Reader reader;
        const std::string data(m_outEndpoint->getSubpath("entwine"));
        reader.parse(data, meta, false);

        const std::string err(reader.getFormattedErrorMessages());
        if (!err.empty()) throw std::runtime_error("Invalid JSON: " + err);
    }

    loadProps(meta);

    m_executor.reset(new Executor(m_structure->is3d()));
    m_originId = m_schema->pdalLayout().findDim("Origin");

    m_registry.reset(
            new Registry(
                *m_outEndpoint,
                *m_schema,
                *m_bbox,
                *m_structure,
                *m_pointPool,
                clipThreads,
                meta));
}

void Builder::save()
{
    // Get our own metadata and the registry's - then serialize.
    Json::Value jsonMeta(saveProps());
    m_registry->save(jsonMeta);
    m_outEndpoint->putSubpath(
            "entwine" + m_structure->subsetPostfix(),
            jsonMeta.toStyledString());
}

void Builder::init()
{
    if (m_reprojection)
    {
        m_srs = pdal::SpatialReference(m_reprojection->out()).getWKT();
    }
    else
    {
        auto localHandle(
                m_arbiter->getLocalHandle(
                    m_manifest->get(0).path(),
                    *m_tmpEndpoint));

        const std::string path(localHandle->localPath());

        auto preview(m_executor->preview(path, nullptr));
        if (preview) m_srs = preview->srs;
        else std::cout << "Could not find an SRS" << std::endl;
    }
}

void Builder::merge()
{
    std::unique_ptr<Manifest> manifest;
    std::unique_ptr<BaseChunk> base;
    std::set<Id> ids;
    const std::size_t baseCount([this]()->std::size_t
    {
        Json::Value meta;
        Json::Reader reader;
        const std::string metaString(m_outEndpoint->getSubpath("entwine-0"));
        reader.parse(metaString, meta, false);

        loadProps(meta);
        const std::size_t baseCount(
            meta["structure"]["subset"]["of"].asUInt64());

        if (!baseCount) throw std::runtime_error("Cannot merge this path");

        return baseCount;
    }());

    for (std::size_t i(0); i < baseCount; ++i)
    {
        std::cout << "\t" << i + 1 << " / " << baseCount << std::endl;
        const std::string postfix("-" + std::to_string(i));

        // Fetch metadata for this segment.
        Json::Value meta;

        {
            Json::Reader reader;
            const std::string metaString(
                    m_outEndpoint->getSubpath("entwine" + postfix));
            reader.parse(metaString, meta, false);
        }

        // Append IDs from this segment.
        const Json::Value& jsonIds(meta["ids"]);
        if (jsonIds.isArray())
        {
            for (Json::ArrayIndex i(0); i < jsonIds.size(); ++i)
            {
                ids.insert(Id(jsonIds[i].asString()));
            }
        }

        std::unique_ptr<std::vector<char>> data(
                new std::vector<char>(
                    m_outEndpoint->getSubpathBinary(
                        m_structure->baseIndexBegin().str() + postfix)));

        std::unique_ptr<BaseChunk> currentBase(
                static_cast<BaseChunk*>(
                    Chunk::create(
                        *m_schema,
                        *m_bbox,
                        *m_structure,
                        *m_pointPool,
                        0,
                        m_structure->baseIndexBegin(),
                        m_structure->baseIndexSpan(),
                        std::move(data)).release()));

        std::unique_ptr<Manifest> currentManifest(
                new Manifest(meta["manifest"]));

        if (i == 0)
        {
            base = std::move(currentBase);
            manifest = std::move(currentManifest);
        }
        else
        {
            base->merge(*currentBase);
            manifest->merge(*currentManifest);
        }
    }

    m_manifest = std::move(manifest);

    m_structure->makeWhole();
    m_subBBox.reset();

    Json::Value jsonMeta(saveProps());
    Json::Value& jsonIds(jsonMeta["ids"]);
    for (const auto& id : ids) jsonIds.append(id.str());

    m_outEndpoint->putSubpath("entwine", jsonMeta.toStyledString());
    base->save(*m_outEndpoint);
}

Json::Value Builder::saveProps() const
{
    Json::Value props;

    props["bbox"] = m_bbox->toJson();
    props["schema"] = m_schema->toJson();
    props["structure"] = m_structure->toJson();
    if (m_reprojection) props["reprojection"] = m_reprojection->toJson();
    props["manifest"] = m_manifest->toJson();
    props["srs"] = m_srs;
    props["compressed"] = m_compress;
    props["trustHeaders"] = m_trustHeaders;

    return props;
}

void Builder::loadProps(const Json::Value& props)
{
    m_bbox.reset(new BBox(props["bbox"]));
    m_schema.reset(new Schema(props["schema"]));
    m_pointPool.reset(new Pools(m_schema->pointSize()));
    m_structure.reset(new Structure(props["structure"], *m_bbox));

    if (props.isMember("reprojection"))
        m_reprojection.reset(new Reprojection(props["reprojection"]));

    m_srs = props["srs"].asString();
    m_manifest.reset(new Manifest(props["manifest"]));
    m_trustHeaders = props["trustHeaders"].asBool();
    m_compress = props["compressed"].asBool();
}

void Builder::prep()
{
    // TODO This should be based on numThreads, and ideally also desired
    // memory consumption.
    if (m_pool->numThreads() == 1) sleepCount = 65536 * 256;

    if (m_tmpEndpoint->isRemote())
    {
        throw std::runtime_error("Tmp path must be local");
    }

    if (!arbiter::fs::mkdirp(m_tmpEndpoint->root()))
    {
        throw std::runtime_error("Couldn't create tmp directory");
    }

    const std::string rootDir(m_outEndpoint->root());
    if (!m_outEndpoint->isRemote() && !arbiter::fs::mkdirp(rootDir))
    {
        throw std::runtime_error("Couldn't create local build directory");
    }
}

void Builder::next()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    ++m_origin;
}

bool Builder::keepGoing() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_origin < m_end;
}

bool Builder::setEnd(const Origin end)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    const bool set(end < m_end && end > m_origin);
    if (set) m_end = end;
    return set;
}

void Builder::stop()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_end = std::min(m_end, m_origin + 1);
    std::cout << "Setting end at " << m_end << std::endl;
}

Origin Builder::end() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_end;
}

} // namespace entwine


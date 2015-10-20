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
#include <entwine/tree/registry.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-table.hpp>
#include <entwine/types/single-point-table.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/executor.hpp>
#include <entwine/util/fs.hpp>

using namespace arbiter;

namespace entwine
{

namespace
{
    const std::size_t sleepCount(65536 * 256);
}

Builder::Builder(
        const std::string outPath,
        const std::string tmpPath,
        const bool compress,
        const bool trustHeaders,
        const Reprojection* reprojection,
        const BBox* bbox,
        const DimList& dimList,
        const std::size_t numThreads,
        const Structure& structure,
        std::shared_ptr<Arbiter> arbiter)
    : m_bbox(bbox ? new BBox(*bbox) : 0)
    , m_subBBox(
            structure.subset() ?
                new BBox(structure.subset()->bbox()) : nullptr)
    , m_schema(new Schema(dimList))
    , m_structure(new Structure(structure))
    , m_reprojection(reprojection ? new Reprojection(*reprojection) : 0)
    , m_manifest(new Manifest())
    , m_mutex()
    , m_stats()
    , m_compress(compress)
    , m_trustHeaders(trustHeaders)
    , m_isContinuation(false)
    , m_pool(new Pool(numThreads))
    , m_executor(new Executor(m_structure->is3d()))
    , m_originId(m_schema->pdalLayout().findDim("Origin"))
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
                *m_pointPool))
{
    prep();
}

Builder::Builder(
        const std::string outPath,
        const std::string tmpPath,
        const std::size_t numThreads,
        std::shared_ptr<Arbiter> arbiter)
    : m_bbox()
    , m_subBBox()
    , m_schema()
    , m_structure()
    , m_reprojection()
    , m_manifest()
    , m_mutex()
    , m_stats()
    , m_trustHeaders(false)
    , m_isContinuation(true)
    , m_pool(new Pool(numThreads))
    , m_executor()
    , m_arbiter(arbiter ? arbiter : std::shared_ptr<Arbiter>(new Arbiter()))
    , m_outEndpoint(new Endpoint(m_arbiter->getEndpoint(outPath)))
    , m_tmpEndpoint(new Endpoint(m_arbiter->getEndpoint(tmpPath)))
    , m_pointPool()
    , m_registry()
{
    prep();
    load();
}

Builder::Builder(const std::string path, std::shared_ptr<Arbiter> arbiter)
    : m_bbox()
    , m_subBBox()
    , m_schema()
    , m_structure()
    , m_reprojection()
    , m_manifest()
    , m_mutex()
    , m_stats()
    , m_trustHeaders(true)
    , m_isContinuation(true)
    , m_pool()
    , m_executor()
    , m_arbiter(arbiter ? arbiter : std::shared_ptr<Arbiter>(new Arbiter()))
    , m_outEndpoint(new Endpoint(m_arbiter->getEndpoint(path)))
    , m_tmpEndpoint()
    , m_pointPool()
    , m_registry()
{ }

Builder::~Builder()
{ }

bool Builder::insert(const std::string path)
{
    if (!m_executor->good(path))
    {
        m_manifest->addOmission(path);
        return false;
    }

    const Origin origin(m_manifest->addOrigin(path));

    if (origin == Manifest::invalidOrigin()) return false;  // Already inserted.

    if (origin == 0)
    {
        const std::string localPath(localize(path, origin));
        infer(localPath);
    }

    std::cout << "Adding " << origin << " - " << path << std::endl;

    m_pool->add([this, origin, path]()
    {
        try
        {
            const std::string localPath(localize(path, origin));

            std::unique_ptr<Clipper> clipper(new Clipper(*this));
            std::unique_ptr<Range> zRangePtr(
                m_structure->is3d() ? 0 : new Range());

            Range* zRange(zRangePtr.get());
            std::size_t count(0);

            SimplePointTable table(m_pointPool->dataPool(), *m_schema);

            auto inserter(
                    [this, origin, &table, &clipper, zRange, &count]
                    (pdal::PointView& view)->void
            {
                count += view.size();
                insert(view, table, origin, clipper.get(), zRange);

                if (count >= sleepCount)
                {
                    count = 0;
                    clipper.reset(new Clipper(*this));
                }
            });

            bool doInsert(true);

            if (m_trustHeaders)
            {
                auto preview(
                    m_executor->preview(localPath, m_reprojection.get()));

                if (preview)
                {
                    if (!preview->bbox.overlaps(*m_bbox))
                    {
                        m_stats.addOutOfBounds(preview->numPoints);
                        doInsert = false;
                    }
                    else if (m_subBBox && !preview->bbox.overlaps(*m_subBBox))
                    {
                        doInsert = false;
                    }
                }
            }

            if (doInsert)
            {
                if (m_executor->run(
                            table,
                            localPath,
                            m_reprojection.get(),
                            inserter))
                {
                    if (zRange)
                    {
                        zRange->min = std::floor(zRange->min);
                        zRange->max = std::ceil(zRange->max);

                        std::lock_guard<std::mutex> lock(m_mutex);
                        m_bbox->growZ(*zRange);
                    }
                }
                else
                {
                    m_manifest->addError(origin);
                }
            }

            const std::size_t mem(Chunk::getChunkMem());
            const std::size_t div(1000000000);

            std::cout << "\tDone " << origin << " - " <<
                "\tGlobal usage: " << mem / div << "." << mem % div <<
                " GB in " << Chunk::getChunkCnt() << " chunks." <<
                std::endl;

            if (m_arbiter->isRemote(path) && !fs::removeFile(localPath))
            {
                std::cout << "Couldn't delete " << localPath << std::endl;
                throw std::runtime_error("Couldn't delete tmp file");
            }
        }
        catch (std::runtime_error e)
        {
            std::cout << "During " << path << ": " << e.what() << std::endl;
            m_manifest->addError(origin);
        }
        catch (...)
        {
            std::cout << "Caught unknown error during " << path << std::endl;
            m_manifest->addError(origin);
        }
    });

    return true;
}

void Builder::insert(
        pdal::PointView& pointView,
        SimplePointTable& table,
        Origin origin,
        Clipper* clipper,
        Range* zRange)
{
    using namespace pdal;

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
                    localView.getFieldAs<double>(Dimension::Id::X, 0),
                    localView.getFieldAs<double>(Dimension::Id::Y, 0),
                    localView.getFieldAs<double>(Dimension::Id::Z, 0)),
                std::move(data));

        const Point& point(info->val().point());

        if (m_bbox->contains(point))
        {
            if (!m_subBBox || m_subBBox->contains(point))
            {
                Climber climber(*m_bbox, *m_structure);

                if (m_registry->addPoint(info, climber, clipper))
                {
                    m_stats.addPoint();

                    if (zRange) zRange->grow(point.z);
                }
                else
                {
                    rejected.push(std::move(info));
                    m_stats.addFallThrough();
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
            m_stats.addOutOfBounds();
        }
    }
}

void Builder::infer(const std::string path)
{
    using namespace pdal;

    auto preview(m_executor->preview(path, m_reprojection.get(), true));

    if (preview)
    {
        m_srs = preview->srs;

        if (m_trustHeaders && !m_bbox && preview->bbox.exists())
        {
            std::cout << "Inferring bounds from header of " << path << "..." <<
                std::endl;

            BBox bbox(preview->bbox);
            m_bbox.reset(
                    new BBox(
                        Point(
                            std::floor(bbox.min().x),
                            std::floor(bbox.min().y),
                            std::floor(bbox.min().z)),
                        Point(
                            std::ceil(bbox.max().x),
                            std::ceil(bbox.max().y),
                            std::ceil(bbox.max().z)),
                        m_structure->is3d()));

            std::cout << "\tGot: " << *m_bbox << "\n" << std::endl;
        }
    }

    if (!m_bbox)
    {
        std::cout << "Inferring bounds from " << path << "..." << std::endl;

        // Use BBox::set() to avoid malformed BBox warning.
        BBox bbox;
        bbox.set(
                Point(
                    std::numeric_limits<double>::max(),
                    std::numeric_limits<double>::max(),
                    std::numeric_limits<double>::max()),
                Point(
                    std::numeric_limits<double>::lowest(),
                    std::numeric_limits<double>::lowest(),
                    std::numeric_limits<double>::lowest()),
                true);

        SimplePointTable table(m_pointPool->dataPool(), *m_schema);
        const std::string localPath(localize(path, 0));

        auto bounder([this, &bbox](pdal::PointView& view)->void
        {
            for (std::size_t i = 0; i < view.size(); ++i)
            {
                bbox.grow(
                        Point(
                            view.getFieldAs<double>(Dimension::Id::X, i),
                            view.getFieldAs<double>(Dimension::Id::Y, i),
                            view.getFieldAs<double>(Dimension::Id::Z, i)));
            }
        });

        if (!m_executor->run(table, localPath, m_reprojection.get(), bounder))
        {
            throw std::runtime_error("Error inferring bounds");
        }

        m_bbox.reset(
                new BBox(
                    Point(
                        std::floor(bbox.min().x),
                        std::floor(bbox.min().y),
                        std::floor(bbox.min().z)),
                    Point(
                        std::ceil(bbox.max().x),
                        std::ceil(bbox.max().y),
                        std::ceil(bbox.max().z)),
                    m_structure->is3d()));

        std::cout << "\tGot: " << *m_bbox << "\n" << std::endl;
    }
}

std::string Builder::localize(const std::string path, const Origin origin)
{
    std::string localPath(arbiter::Arbiter::stripType(path));

    if (m_arbiter->isRemote(path))
    {
        std::size_t dot(path.find_last_of("."));

        if (dot != std::string::npos)
        {
            const std::string subpath(
                    name() + "-" + std::to_string(origin) + path.substr(dot));

            m_tmpEndpoint->putSubpath(
                    subpath,
                    m_arbiter->getBinary(path));

            localPath = m_tmpEndpoint->fullPath(subpath);
        }
        else
        {
            throw std::runtime_error("Bad extension on: " + path);
        }
    }

    return localPath;
}

void Builder::clip(
        const Id& index,
        const std::size_t chunkNum,
        Clipper* clipper)
{
    m_registry->clip(index, chunkNum, clipper);
}

void Builder::join()
{
    m_pool->join();
}

void Builder::load()
{
    Json::Value meta;

    {
        Json::Reader reader;
        const std::string data(m_outEndpoint->getSubpath("entwine"));
        reader.parse(data, meta, false);
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
                meta));
}

void Builder::merge()
{
    std::unique_ptr<BaseChunk> base;
    std::vector<Id> ids;
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
            for (std::size_t i(0); i < jsonIds.size(); ++i)
            {
                Json::ArrayIndex arrayIndex(static_cast<Json::ArrayIndex>(i));

                ids.push_back(Id(jsonIds[arrayIndex].asString()));
            }
        }
        else
        {
            throw std::runtime_error("Invalid IDs.");
        }

        std::vector<char> data(
                m_outEndpoint->getSubpathBinary(
                    m_structure->baseIndexBegin().str() + postfix));

        if (i == 0)
        {
            std::cout << "\t1 / " << baseCount << std::endl;
            base.reset(
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
        }
        else
        {
            std::cout << "\t" << i + 1 << " / " << baseCount << std::endl;

            std::unique_ptr<BaseChunk> chunkData(
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

            // Update stats.  Don't add numOutOfBounds, since those are
            // based on the global bounds, so every segment's out-of-bounds
            // count should be equal.
            Stats stats(meta["stats"]);
            m_stats.addPoint(stats.getNumPoints());
            m_stats.addFallThrough(stats.getNumFallThroughs());
            if (m_stats.getNumOutOfBounds() != stats.getNumOutOfBounds())
            {
                throw std::runtime_error("Invalid stats in segment.");
            }

            base->merge(*chunkData);
        }
    }

    m_structure->makeWhole();
    m_subBBox.reset();

    Json::Value jsonMeta(saveProps());
    Json::Value& jsonIds(jsonMeta["ids"]);

    for (auto id : ids)
    {
        jsonIds.append(id.str());
    }

    m_outEndpoint->putSubpath("entwine", jsonMeta.toStyledString());

    base->save(*m_outEndpoint);
}

void Builder::link(std::vector<std::string> subsetPaths)
{
    /*
    std::unique_ptr<ContiguousChunk> base;
    std::vector<Endpoint> subs;
    std::map<std::string, std::vector<std::size_t>> ids;

    for (std::size_t i(0); i < subsetPaths.size(); ++i)
    {
        subs.push_back(m_arbiter->getEndpoint(subsetPaths[i]));
    }

    const std::size_t baseCount([this, &subs]()->std::size_t
    {
        Json::Value meta;
        Json::Reader reader;
        const std::string metaString(subs[0].getSubpath("entwine-0"));
        reader.parse(metaString, meta, false);

        loadProps(meta);
        const std::size_t baseCount(meta["structure"]["subset"][1].asUInt64());

        if (!baseCount)
        {
            throw std::runtime_error("Cannot link this path");
        }
        else if (baseCount != subs.size())
        {
            throw std::runtime_error("Invalid number of subsets to link");
        }

        return baseCount;
    }());

    for (std::size_t i(0); i < baseCount; ++i)
    {
        const std::string postfix("-" + std::to_string(i));

        // Fetch metadata for this segment.
        Json::Value meta;

        {
            Json::Reader reader;
            const std::string metaString(
                    subs[i].getSubpath("entwine" + postfix));
            reader.parse(metaString, meta, false);
        }

        // Append IDs from this segment.
        const Json::Value& jsonIds(meta["ids"]);
        std::vector<std::size_t>& subIds(ids[subsetPaths[i]]);

        if (jsonIds.isArray())
        {
            for (std::size_t i(0); i < jsonIds.size(); ++i)
            {
                subIds.push_back(
                        jsonIds[static_cast<Json::ArrayIndex>(i)].asUInt64());
            }
        }
        else
        {
            throw std::runtime_error("Invalid IDs.");
        }

        std::vector<char> data(subs[i].getSubpathBinary(
                m_structure->baseIndexBegin().str() + postfix));

        if (!data.empty() || data.back() == Contiguous)
        {
            data.pop_back();

            if (i == 0)
            {
                std::cout << "\t1 / " << baseCount << std::endl;
                base.reset(
                        new ContiguousChunk(
                            *m_schema,
                            m_structure->baseIndexBegin(),
                            m_structure->baseIndexSpan(),
                            data));
            }
            else
            {
                std::cout << "\t" << i + 1 << " / " << baseCount << std::endl;

                std::unique_ptr<ContiguousChunk> chunkData(
                        new ContiguousChunk(
                            *m_schema,
                            m_structure->baseIndexBegin(),
                            m_structure->baseIndexSpan(),
                            data));

                // Update stats.  Don't add numOutOfBounds, since those are
                // based on the global bounds, so every segment's out-of-bounds
                // count should be equal.
                Stats stats(meta["stats"]);
                m_stats.addPoint(stats.getNumPoints());
                m_stats.addFallThrough(stats.getNumFallThroughs());
                if (m_stats.getNumOutOfBounds() != stats.getNumOutOfBounds())
                {
                    std::cout << "\t\tFound conflicting stats." << std::endl;
                }

                base->merge(*chunkData);
            }
        }
        else
        {
            throw std::runtime_error("Invalid base segment.");
        }
    }

    std::cout << "Building aggregated meta..." << std::endl;

    m_structure->makeWhole();
    m_subBBox.reset();

    Json::Value jsonMeta(saveProps());
    Json::Value& jsonIds(jsonMeta["ids"]);

    for (const auto& sub : ids)
    {
        std::string path(sub.first);
        const std::vector<std::size_t>& subIds(sub.second);
        Json::Value& jsonSubIds(jsonIds[path]);

        for (auto id : subIds)
        {
            jsonSubIds.append(static_cast<Json::UInt64>(id));
        }
    }

    std::cout << "Saving aggregated meta..." << std::endl;
    m_outEndpoint->putSubpath("entwine", jsonMeta.toStyledString());

    std::cout << "Saving base data..." << std::endl;
    base->save(*m_outEndpoint);
    */
}

void Builder::save()
{
    // Ensure constant state, waiting for all worker threads to complete.
    join();

    // Get our own metadata and the registry's - then serialize.
    Json::Value jsonMeta(saveProps());
    m_registry->save(jsonMeta);
    m_outEndpoint->putSubpath(
            "entwine" + m_structure->subsetPostfix(),
            jsonMeta.toStyledString());

    // Re-allow inserts.
    m_pool->go();
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
    props["stats"] = m_stats.toJson();
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
    m_stats = Stats(props["stats"]);
    m_trustHeaders = props["trustHeaders"].asBool();
    m_compress = props["compressed"].asBool();
}

void Builder::prep()
{
    if (m_tmpEndpoint->isRemote())
    {
        throw std::runtime_error("Tmp path must be local");
    }

    if (!fs::mkdirp(m_tmpEndpoint->root()))
    {
        throw std::runtime_error("Couldn't create tmp directory");
    }

    if (!m_outEndpoint->isRemote() && !fs::mkdirp(m_outEndpoint->root()))
    {
        throw std::runtime_error("Couldn't create local build directory");
    }
}

std::string Builder::name() const
{
    std::string name(m_outEndpoint->root());

    // TODO Temporary/hacky.
    const std::size_t pos(name.find_last_of("/\\"));

    if (pos != std::string::npos)
    {
        name = name.substr(pos + 1);
    }

    return name;
}

} // namespace entwine


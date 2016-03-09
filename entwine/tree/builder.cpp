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

#include <chrono>
#include <limits>
#include <thread>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/third/splice-pool/splice-pool.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/tree/hierarchy.hpp>
#include <entwine/tree/registry.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/executor.hpp>
#include <entwine/util/pool.hpp>

using namespace arbiter;

namespace entwine
{

namespace
{
    const double workToClipRatio(0.33);
    const std::size_t sleepCount(65536 * 24);
    // const std::size_t hierarchyCount(65536);

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
        const Subset* subset,
        const Reprojection* reprojection,
        const BBox& bboxConforming,
        const Schema& schema,
        const std::size_t totalThreads,
        const float threshold,
        const Structure& structure,
        std::shared_ptr<Arbiter> arbiter)
    : m_bboxConforming(new BBox(bboxConforming))
    , m_bbox()
    , m_subBBox(subset ? new BBox(subset->bbox()) : nullptr)
    , m_schema(new Schema(schema))
    , m_structure(new Structure(structure))
    , m_manifest(std::move(manifest))
    , m_subset(subset ? new Subset(*subset) : nullptr)
    , m_reprojection(reprojection ? new Reprojection(*reprojection) : nullptr)
    , m_mutex()
    , m_compress(compress)
    , m_trustHeaders(trustHeaders)
    , m_isContinuation(false)
    , m_srs()
    , m_pool(new Pool(getWorkThreads(totalThreads)))
    , m_initialWorkThreads(getWorkThreads(totalThreads))
    , m_initialClipThreads(getClipThreads(totalThreads))
    , m_totalThreads(totalThreads)
    , m_threshold(threshold)
    , m_usage(0)
    , m_executor(new Executor(m_structure->is3d()))
    , m_originId(m_schema->pdalLayout().findDim("Origin"))
    , m_origin(0)
    , m_end(0)
    , m_added(0)
    , m_arbiter(arbiter ? arbiter : std::shared_ptr<Arbiter>(new Arbiter()))
    , m_outEndpoint(new Endpoint(m_arbiter->getEndpoint(outPath)))
    , m_tmpEndpoint(new Endpoint(m_arbiter->getEndpoint(tmpPath)))
    , m_pointPool(new Pools(*m_schema))
    , m_registry()
    , m_hierarchy(new Hierarchy(*m_bbox))
{
    m_bboxConforming->growBy(0.005);
    m_bbox.reset(new BBox(*m_bboxConforming));

    if (!m_bbox->isCubic())
    {
        m_bbox->cubeify();
    }

    m_registry.reset(new Registry(*m_outEndpoint, *this, m_initialClipThreads));
    prep();
}

Builder::Builder(
        const std::string outPath,
        const std::string tmpPath,
        const std::size_t totalThreads,
        const float threshold,
        std::shared_ptr<Arbiter> arbiter)
    : m_bboxConforming()
    , m_bbox()
    , m_subBBox()
    , m_schema()
    , m_structure()
    , m_manifest()
    , m_subset()
    , m_reprojection()
    , m_mutex()
    , m_compress(true)
    , m_trustHeaders(false)
    , m_isContinuation(true)
    , m_srs()
    , m_pool(new Pool(getWorkThreads(totalThreads)))
    , m_initialWorkThreads(getWorkThreads(totalThreads))
    , m_initialClipThreads(getClipThreads(totalThreads))
    , m_totalThreads(totalThreads)
    , m_threshold(threshold)
    , m_usage(0)
    , m_executor()
    , m_originId()
    , m_origin(0)
    , m_end(0)
    , m_added(0)
    , m_arbiter(arbiter ? arbiter : std::shared_ptr<Arbiter>(new Arbiter()))
    , m_outEndpoint(new Endpoint(m_arbiter->getEndpoint(outPath)))
    , m_tmpEndpoint(new Endpoint(m_arbiter->getEndpoint(tmpPath)))
    , m_pointPool()
    , m_registry()
    , m_hierarchy()
{
    prep();
    load(m_initialClipThreads);
}

Builder::Builder(
        const std::string path,
        const std::size_t* subsetId,
        const std::size_t* splitBegin,
        std::shared_ptr<arbiter::Arbiter> arbiter)
    : m_bboxConforming()
    , m_bbox()
    , m_subBBox()
    , m_schema()
    , m_structure()
    , m_manifest()
    , m_subset()
    , m_reprojection()
    , m_mutex()
    , m_compress(true)
    , m_trustHeaders(true)
    , m_isContinuation(true)
    , m_srs()
    , m_pool()
    , m_initialWorkThreads(0)
    , m_initialClipThreads(0)
    , m_totalThreads(0)
    , m_threshold(2.0)
    , m_usage(0)
    , m_executor()
    , m_originId()
    , m_origin(0)
    , m_end(0)
    , m_added(0)
    , m_arbiter(arbiter ? arbiter : std::shared_ptr<Arbiter>(new Arbiter()))
    , m_outEndpoint(new Endpoint(m_arbiter->getEndpoint(path)))
    , m_tmpEndpoint()
    , m_pointPool()
    , m_registry()
    , m_hierarchy()
{
    prep();
    load(subsetId, splitBegin);
}

std::unique_ptr<Builder> Builder::create(
        const std::string path,
        std::shared_ptr<arbiter::Arbiter> arbiter)
{
    std::unique_ptr<Builder> builder;
    const std::size_t zero(0);

    // Try a subset, but not split, build.
    try { builder.reset(new Builder(path, &zero, nullptr, arbiter)); }
    catch (...) { builder.reset(); }
    if (builder) return builder;

    // Try a split, but not subset, build.
    try { builder.reset(new Builder(path, nullptr, &zero, arbiter)); }
    catch (...) { builder.reset(); }
    if (builder) return builder;

    // Try a split and subset build.
    try { builder.reset(new Builder(path, &zero, &zero, arbiter)); }
    catch (...) { builder.reset(); }
    if (builder) return builder;

    return builder;
}

std::unique_ptr<Builder> Builder::create(
        const std::string path,
        const std::size_t subsetId,
        std::shared_ptr<arbiter::Arbiter> arbiter)
{
    std::unique_ptr<Builder> builder;
    const std::size_t zero(0);

    // Try a subset, but not split, build.
    try { builder.reset(new Builder(path, &subsetId, nullptr, arbiter)); }
    catch (...) { builder.reset(); }
    if (builder) return builder;

    // Try a split and subset build.
    try { builder.reset(new Builder(path, &subsetId, &zero, arbiter)); }
    catch (...) { builder.reset(); }
    if (builder) return builder;

    return builder;
}

Builder::~Builder()
{ }

void Builder::go(std::size_t max)
{
    if (!m_tmpEndpoint)
    {
        throw std::runtime_error("Cannot add to read-only builder");
    }

    m_end = m_manifest->size();

    if (const Manifest::Split* split = m_manifest->split())
    {
        m_origin = split->begin();
        m_end = split->end();
    }

    max = max ? std::min<std::size_t>(m_end, max) : m_end;

    while (keepGoing() && m_added < max)
    {
        FileInfo& info(m_manifest->get(m_origin));
        const std::string path(info.path());

        if (!checkInfo(info))
        {
            std::cout << "Skipping " << m_origin << " - " << path << std::endl;
            next();
            continue;
        }

        ++m_added;
        std::cout << "Adding " << m_origin << " - " << path << std::endl;
        const auto origin(m_origin);

        m_pool->add([this, origin, &info, path]()
        {
            FileInfo::Status status(FileInfo::Status::Inserted);

            try
            {
                insertPath(origin, info);
            }
            catch (const std::runtime_error& e)
            {
                std::cout << "During " << path << ": " << e.what() << std::endl;
                status = FileInfo::Status::Error;

                addError(path, e.what());
            }
            catch (...)
            {
                std::cout << "Unknown error during " << path << std::endl;
                status = FileInfo::Status::Error;

                addError(path, "Unknown error");
            }

            m_manifest->set(origin, status);
        });

        next();
    }

    std::cout << "\tPushes complete - joining..." << std::endl;
    m_pool->join();
    std::cout << "\tJoined - saving..." << std::endl;
    save();
}

bool Builder::checkInfo(const FileInfo& info)
{
    if (info.status() != FileInfo::Status::Outstanding)
    {
        return false;
    }
    else if (!m_executor->good(info.path()))
    {
        m_manifest->set(m_origin, FileInfo::Status::Omitted);
        return false;
    }
    else if (const BBox* bbox = info.bbox())
    {
        if (!checkBounds(m_origin, *bbox, info.numPoints()))
        {
            m_manifest->set(m_origin, FileInfo::Status::Inserted);
            return false;
        }
    }

    return true;
}

bool Builder::checkBounds(
        const Origin origin,
        const BBox& bbox,
        const std::size_t numPoints)
{
    if (!bbox.overlaps(*m_bbox))
    {
        const bool primary(!m_subset || m_subset->primary());
        m_manifest->addOutOfBounds(origin, numPoints, primary);
        return false;
    }
    else if (m_subBBox && !bbox.overlaps(*m_subBBox))
    {
        return false;
    }

    return true;
}

bool Builder::insertPath(const Origin origin, FileInfo& info)
{
    auto localHandle(m_arbiter->getLocalHandle(info.path(), *m_tmpEndpoint));
    const std::string& localPath(localHandle->localPath());

    // If we don't have an inferred bbox, check against the actual file.
    if (!info.bbox())
    {
        auto pre(m_executor->preview(localPath, m_reprojection.get()));

        if (pre && !checkBounds(origin, pre->bbox, pre->numPoints))
        {
            return false;
        }
    }

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_srs.empty())
        {
            if (m_reprojection)
            {
                // Use the executor's lock here, since we are likely to be
                // fighting against concurrent Executor::preview()/run() calls.
                auto lock(m_executor->getLock());
                m_srs = pdal::SpatialReference(m_reprojection->out()).getWKT();
            }
            else
            {
                auto preview(m_executor->preview(localPath, nullptr));
                if (preview) m_srs = preview->srs;
            }

            if (m_srs.size()) std::cout << "Found an SRS" << std::endl;
        }
    }

    std::size_t s(0);

    Clipper clipper(*this, origin);

    Hierarchy localHierarchy(*m_bbox);
    Climber climber(*m_bbox, *m_structure, &localHierarchy);

    auto inserter([this, origin, &clipper, &climber, &s]
    (PooledInfoStack infoStack)
    {
        s += infoStack.size();

        if (s > sleepCount)
        {
            s = 0;
            clipper.clip();
        }

        return insertData(std::move(infoStack), origin, clipper, climber);
    });

    PooledPointTable table(*m_pointPool, inserter);

    const bool result(m_executor->run(table, localPath, m_reprojection.get()));

    std::lock_guard<std::mutex> lock(m_mutex);
    m_hierarchy->merge(localHierarchy);
    return result;
}

PooledInfoStack Builder::insertData(
        PooledInfoStack infoStack,
        const Origin origin,
        Clipper& clipper,
        Climber& climber)
{
    PointStats pointStats;
    PooledInfoStack rejected(m_pointPool->infoPool());

    auto reject([&rejected](PooledInfoNode& info)
    {
        rejected.push(std::move(info));
    });

    while (!infoStack.empty())
    {
        PooledInfoNode info(infoStack.popOne());
        const Point& point(info->val().point());

        if (m_bboxConforming->contains(point))
        {
            if (!m_subBBox || m_subBBox->contains(point))
            {
                climber.reset();

                if (m_registry->addPoint(info, climber, clipper))
                {
                    pointStats.addInsert();
                }
                else
                {
                    reject(info);
                    pointStats.addOverflow();
                }
            }
            else
            {
                reject(info);
            }
        }
        else
        {
            reject(info);
            pointStats.addOutOfBounds();
        }
    }

    m_manifest->add(origin, pointStats);

    return rejected;
}

void Builder::load(const std::size_t clipThreads, const std::string post)
{
    Json::Value meta;
    Json::Reader reader;

    auto check([&reader]()
    {
        const std::string err(reader.getFormattedErrorMessages());
        if (!err.empty()) throw std::runtime_error("Invalid JSON: " + err);
    });

    {
        // Get top-level metadata.
        const std::string strMeta(m_outEndpoint->getSubpath("entwine" + post));
        reader.parse(strMeta, meta, false);
        check();

        // For Reader invocation only.
        if (post.empty())
        {
            m_numPointsClone = meta["numPoints"].asUInt64();
        }
    }

    {
        const std::string strIds(
                m_outEndpoint->getSubpath("entwine-ids" + post));
        reader.parse(strIds, meta["ids"], false);
        check();
    }

    {
        const std::string strHierarchy(
                m_outEndpoint->getSubpath("entwine-hierarchy" + post));
        reader.parse(strHierarchy, meta["hierarchy"], false);
        check();
    }

    {
        const std::string strManifest(
                m_outEndpoint->getSubpath("entwine-manifest" + post));
        reader.parse(strManifest, meta["manifest"], false);
        check();
    }

    loadProps(meta);

    m_executor.reset(new Executor(m_structure->is3d()));
    m_originId = m_schema->pdalLayout().findDim("Origin");

    m_registry.reset(
            new Registry(*m_outEndpoint, *this, clipThreads, meta["ids"]));
}

void Builder::load(const std::size_t* subsetId, const std::size_t* splitBegin)
{
    std::string post(
            (subsetId ? "-" + std::to_string(*subsetId) : "") +
            (splitBegin ? "-" + std::to_string(*splitBegin) : ""));

    load(0, post);
}

void Builder::save()
{
    m_registry->save();
    const auto pf(postfix());

    Json::FastWriter writer;

    {
        Json::Value jsonMeta(saveOwnProps());
        m_outEndpoint->putSubpath("entwine" + pf, jsonMeta.toStyledString());
    }

    {
        Json::Value jsonIds(m_registry->toJson());
        m_outEndpoint->putSubpath("entwine-ids" + pf, writer.write(jsonIds));
    }

    {
        Json::Value jsonHierarchy(m_hierarchy->toJson());
        m_outEndpoint->putSubpath(
                "entwine-hierarchy" + pf,
                writer.write(jsonHierarchy));
    }

    {
        Json::Value jsonManifest(m_manifest->toJson());
        m_outEndpoint->putSubpath(
                "entwine-manifest" + pf,
                writer.write(jsonManifest));
    }
}

void Builder::unsplit(Builder& other)
{
    std::cout << "Unsplitting " << m_manifest->split()->toJson() << std::endl;
    std::cout << "With " << other.manifest().split()->toJson() << std::endl;
}

void Builder::merge(Builder& other)
{
    if (!subset() && !m_manifest->split())
    {
        throw std::runtime_error("Cannot merge non-subset build");
    }

    if (m_srs.empty() && !other.srs().empty())
    {
        m_srs = other.srs();
    }

    m_registry->merge(other.registry());
    m_manifest->merge(other.manifest());
}

Json::Value Builder::saveOwnProps() const
{
    Json::Value props;

    props["bboxConforming"] = m_bboxConforming->toJson();
    props["bbox"] = m_bbox->toJson();
    props["schema"] = m_schema->toJson();
    props["structure"] = m_structure->toJson();

    // The infallible numPoints value is in the manifest, which is stored
    // elsewhere to avoid the Reader needing it.  For the Reader, then,
    // duplicate that info here.
    props["numPoints"] =
        static_cast<Json::UInt64>(m_manifest->pointStats().inserts());

    if (m_subset) props["subset"] = m_subset->toJson();
    if (m_reprojection) props["reprojection"] = m_reprojection->toJson();

    props["srs"] = m_srs;
    props["compressed"] = m_compress;
    props["trustHeaders"] = m_trustHeaders;

    return props;
}

void Builder::loadProps(Json::Value& props)
{
    m_bboxConforming.reset(new BBox(props["bboxConforming"]));
    m_bbox.reset(new BBox(props["bbox"]));
    m_schema.reset(new Schema(props["schema"]));
    m_pointPool.reset(new Pools(*m_schema));
    m_structure.reset(new Structure(props["structure"]));
    m_hierarchy.reset(new Hierarchy(*m_bbox, props["hierarchy"]));

    if (props.isMember("subset"))
    {
        m_subset.reset(new Subset(*m_structure, *m_bbox, props["subset"]));
    }

    if (props.isMember("reprojection"))
    {
        m_reprojection.reset(new Reprojection(props["reprojection"]));
    }

    m_srs = props["srs"].asString();
    m_manifest.reset(new Manifest(props["manifest"]));
    m_compress = props["compressed"].asBool();
    m_trustHeaders = props["trustHeaders"].asBool();
}

void Builder::prep()
{
    if (m_tmpEndpoint)
    {
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
}

std::string Builder::postfix(const bool includeSubset) const
{
    // When saving individual chunks, we don't care about subsetting since
    // the chunks are spatially disparate anyway (except for the base chunk).
    return
        (includeSubset && m_subset ? m_subset->basePostfix() : "") +
        (m_manifest->splitPostfix());
}

void Builder::stop()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_end = std::min(m_end, m_origin + 1);
    std::cout << "Setting end at " << m_end << std::endl;
}

void Builder::makeWhole()
{
    if (m_subset) m_subset.reset();
    m_subBBox.reset();
}

const std::vector<std::string>& Builder::errors() const
{
    return m_errors;
}

std::unique_ptr<Manifest::Split> Builder::takeWork()
{
    std::unique_ptr<Manifest::Split> manifestSplit;

    std::lock_guard<std::mutex> lock(m_mutex);

    const std::size_t remaining(m_end - m_origin);
    const double ratioRemaining(
            static_cast<double>(remaining) /
            static_cast<double>(m_manifest->size()));

    if (remaining > 2 && ratioRemaining > 0.05)
    {
        m_end = m_origin + remaining / 2;
        manifestSplit = m_manifest->split(m_end);
    }

    return manifestSplit;
}

Origin Builder::end() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_end;
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

const BBox& Builder::bboxConforming() const { return *m_bboxConforming; }
const BBox& Builder::bbox() const           { return *m_bbox; }
const Schema& Builder::schema() const       { return *m_schema; }
const Manifest& Builder::manifest() const   { return *m_manifest; }
const Structure& Builder::structure() const { return *m_structure; }
const Registry& Builder::registry() const   { return *m_registry; }
const Hierarchy& Builder::hierarchy() const { return *m_hierarchy; }
const Subset* Builder::subset() const       { return m_subset.get(); }

const Reprojection* Builder::reprojection() const
{
    return m_reprojection.get();
}

Pools& Builder::pools() const { return *m_pointPool; }

const arbiter::Endpoint& Builder::outEndpoint() const { return *m_outEndpoint; }
const arbiter::Endpoint& Builder::tmpEndpoint() const { return *m_tmpEndpoint; }

void Builder::clip(
        const Id& index,
        const std::size_t chunkNum,
        const std::size_t id)
{
    m_registry->clip(index, chunkNum, id);
}

void Builder::addError(const std::string& path, const std::string& error)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    const std::size_t lastSlash(path.find_last_of('/'));
    const std::string file(
            lastSlash != std::string::npos ? path.substr(lastSlash + 1) : path);
    m_errors.push_back(file + ": " + error);
}

float Builder::chunkMem() const
{
    if (usage()) return usage();

    static const std::size_t bytesPerPoint(
            m_schema->pointSize() + sizeof(Tube));

    return
        static_cast<float>(Chunk::getChunkMem() * bytesPerPoint) * 1.6 /
        1073741824.0;
}

} // namespace entwine


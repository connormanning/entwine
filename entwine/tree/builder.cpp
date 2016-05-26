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
#include <numeric>
#include <random>
#include <thread>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/third/splice-pool/splice-pool.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/tree/registry.hpp>
#include <entwine/tree/tiler.hpp>
#include <entwine/tree/traverser.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/compression.hpp>
#include <entwine/util/executor.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

using namespace arbiter;

namespace
{
    const double workToClipRatio(0.33);
    const std::size_t sleepCount(65536 * 24);

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
        const Structure& structure,
        const OuterScope outerScope)
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
    , m_executor(new Executor(m_structure->is3d()))
    , m_originId(m_schema->pdalLayout().findDim("Origin"))
    , m_origin(0)
    , m_end(0)
    , m_added(0)
    , m_arbiter(outerScope.getArbiter())
    , m_outEndpoint(new Endpoint(m_arbiter->getEndpoint(outPath)))
    , m_tmpEndpoint(new Endpoint(m_arbiter->getEndpoint(tmpPath)))
    , m_pointPool(outerScope.getPointPool(*m_schema))
    , m_nodePool(outerScope.getNodePool())
    , m_registry()
    , m_hierarchy(new Hierarchy(*m_bbox, *m_nodePool))
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
        const std::string pf,
        const Json::Value subsetJson,
        const OuterScope outerScope)
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
    , m_executor()
    , m_originId()
    , m_origin(0)
    , m_end(0)
    , m_added(0)
    , m_arbiter(outerScope.getArbiter())
    , m_outEndpoint(new Endpoint(m_arbiter->getEndpoint(outPath)))
    , m_tmpEndpoint(new Endpoint(m_arbiter->getEndpoint(tmpPath)))
    , m_pointPool()
    , m_nodePool()
    , m_registry()
    , m_hierarchy()
{
    prep();
    load(outerScope, m_initialClipThreads, pf);

    if (!subsetJson.empty())
    {
        m_subset.reset(new Subset(*m_structure, *m_bbox, subsetJson));
        m_subBBox.reset(new BBox(m_subset->bbox()));
    }

    m_hierarchy->awakenAll();
}

Builder::Builder(
        const std::string path,
        const std::size_t totalThreads,
        const std::size_t* subsetId,
        const std::size_t* splitBegin,
        const OuterScope outerScope)
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
    , m_pool(new Pool(getWorkThreads(totalThreads)))
    , m_initialWorkThreads(getWorkThreads(totalThreads))
    , m_initialClipThreads(getClipThreads(totalThreads))
    , m_totalThreads(0)
    , m_executor()
    , m_originId()
    , m_origin(0)
    , m_end(0)
    , m_added(0)
    , m_arbiter(outerScope.getArbiter())
    , m_outEndpoint(new Endpoint(m_arbiter->getEndpoint(path)))
    , m_tmpEndpoint()
    , m_pointPool()
    , m_nodePool()
    , m_registry()
    , m_hierarchy()
{
    prep();
    load(outerScope, subsetId, splitBegin);
}

std::unique_ptr<Builder> Builder::create(
        const std::string path,
        const std::size_t threads,
        const OuterScope os)
{
    std::unique_ptr<Builder> builder;
    const std::size_t zero(0);

    // Try a subset, but not split, build.
    try { builder.reset(new Builder(path, threads, &zero, nullptr, os)); }
    catch (...) { builder.reset(); }
    if (builder) return builder;

    // Try a split, but not subset, build.
    try { builder.reset(new Builder(path, threads, nullptr, &zero, os)); }
    catch (...) { builder.reset(); }
    if (builder) return builder;

    // Try a split and subset build.
    try { builder.reset(new Builder(path, threads, &zero, &zero, os)); }
    catch (...) { builder.reset(); }
    if (builder) return builder;

    return builder;
}

std::unique_ptr<Builder> Builder::create(
        const std::string path,
        const std::size_t threads,
        const std::size_t subsetId,
        const OuterScope os)
{
    std::unique_ptr<Builder> b;
    const std::size_t zero(0);

    // Try a subset, but not split, build.
    try { b.reset(new Builder(path, threads, &subsetId, nullptr, os)); }
    catch (...) { }
    if (b) return b;

    // Try a split and subset build.
    try { b.reset(new Builder(path, threads, &subsetId, &zero, os)); }
    catch (...) { }
    if (b) return b;

    return b;
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
                // Don't construct the pdal::SpatialReference ourself, since
                // we need to use the Executors lock to do so.
                m_srs = m_executor->getSrsString(m_reprojection->out());
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

    Hierarchy localHierarchy(*m_bbox, *m_nodePool);
    Climber climber(*m_bbox, *m_structure, &localHierarchy);

    auto inserter([this, origin, &clipper, &climber, &s]
    (Cell::PooledStack cells)
    {
        s += cells.size();

        if (s > sleepCount)
        {
            s = 0;
            clipper.clip();
        }

        return insertData(std::move(cells), origin, clipper, climber);
    });

    PooledPointTable table(*m_pointPool, inserter, m_originId, origin);

    const bool result(m_executor->run(table, localPath, m_reprojection.get()));

    std::lock_guard<std::mutex> lock(m_mutex);
    m_hierarchy->merge(localHierarchy);
    return result;
}

Cell::PooledStack Builder::insertData(
        Cell::PooledStack cells,
        const Origin origin,
        Clipper& clipper,
        Climber& climber)
{
    PointStats pointStats;
    Cell::PooledStack rejected(m_pointPool->cellPool());

    auto reject([&rejected](Cell::PooledNode& cell)
    {
        rejected.push(std::move(cell));
    });

    while (!cells.empty())
    {
        Cell::PooledNode cell(cells.popOne());
        const Point& point(cell->point());

        if (m_bboxConforming->contains(point))
        {
            if (!m_subBBox || m_subBBox->contains(point))
            {
                climber.reset();
                climber.magnifyTo(point, m_structure->baseDepthBegin());

                if (m_registry->addPoint(cell, climber, clipper))
                {
                    pointStats.addInsert();
                }
                else
                {
                    reject(cell);
                    pointStats.addOverflow();
                }
            }
            else
            {
                reject(cell);
            }
        }
        else
        {
            reject(cell);
            pointStats.addOutOfBounds();
        }
    }

    if (origin != invalidOrigin) m_manifest->add(origin, pointStats);

    return rejected;
}

void Builder::load(
        const OuterScope outerScope,
        const std::size_t clipThreads,
        const std::string pf)
{
    Json::Value meta;
    Json::Reader reader;

    auto error([&reader]()
    {
        throw std::runtime_error(
            "Invalid JSON: " + reader.getFormattedErrorMessages());
    });

    {
        // Get top-level metadata.
        const std::string strMeta(m_outEndpoint->getSubpath("entwine" + pf));

        if (!reader.parse(strMeta, meta, false)) error();

        // For Reader invocation only.
        if (pf.empty())
        {
            m_numPointsClone = meta["numPoints"].asUInt64();
        }
    }

    {
        const std::string strIds(
                m_outEndpoint->getSubpath("entwine-ids" + pf));

        if (!reader.parse(strIds, meta["ids"], false)) error();
    }

    {
        const std::string strManifest(
                m_outEndpoint->getSubpath("entwine-manifest" + pf));

        if (!reader.parse(strManifest, meta["manifest"], false)) error();
    }

    loadProps(outerScope, meta, pf);

    m_executor.reset(new Executor(m_structure->is3d()));
    m_originId = m_schema->pdalLayout().findDim("Origin");

    m_registry.reset(
            new Registry(
                *m_outEndpoint,
                *this,
                clipThreads,
                meta["ids"]));
}

void Builder::load(
        const OuterScope outerScope,
        const std::size_t* subsetId,
        const std::size_t* splitBegin)
{
    std::string post(
            (subsetId ? "-" + std::to_string(*subsetId) : "") +
            (splitBegin ? "-" + std::to_string(*splitBegin) : ""));

    load(outerScope, 0, post);
}

void Builder::save()
{
    const auto pf(postfix());
    const auto props(propsToSave());
    for (const auto& p : props) m_outEndpoint->putSubpath(p.first, p.second);
}

std::map<std::string, std::string> Builder::propsToSave() const
{
    std::map<std::string, std::string> props;

    const auto pf(postfix());
    Json::FastWriter writer;

    {
        Json::Value jsonMeta(saveOwnProps());
        props["entwine" + pf] = jsonMeta.toStyledString();
    }

    {
        Json::Value jsonIds(m_registry->toJson());
        props["entwine-ids" + pf] = writer.write(jsonIds);
    }

    {
        Json::Value jsonManifest(m_manifest->toJson());
        props["entwine-manifest" + pf] = writer.write(jsonManifest);
    }

    return props;
}

void Builder::unsplit(Builder& other)
{
    Reserves reserves;

    auto countReserves([&reserves]()->std::size_t
    {
        return std::accumulate(
            reserves.begin(),
            reserves.end(),
            0,
            [](const std::size_t size, const Reserves::value_type& r)
            {
                return size + r.second.size();
            });
    });

    if (BaseChunk* otherBase = other.m_registry->base())
    {
        PointStatsMap pointStatsMap;
        Hierarchy localHierarchy(*m_bbox, *m_nodePool);
        Clipper clipper(*this, 0);

        insertHinted(
                reserves,
                otherBase->acquire(),
                pointStatsMap,
                clipper,
                localHierarchy,
                Id(0),
                m_structure->baseDepthBegin(),
                m_structure->baseDepthEnd());

        m_manifest->add(pointStatsMap);
        m_hierarchy->merge(localHierarchy);
    }

    std::cout << "Base overflow: " << countReserves() << "\n" << std::endl;

    BinaryPointTable binaryTable(*m_schema);
    pdal::PointRef pointRef(binaryTable, 0);
    PointStatsMap pointStatsMap;

    const std::set<Id> otherIds(other.registry().ids());

    Traverser traverser(*this, &otherIds);
    traverser.tree([&](std::unique_ptr<Branch> branch)
    {
        Branch* rawBranch(branch.release());

        m_pool->add([&]()
        {
            std::unique_ptr<Branch> branch(rawBranch);
            const auto cold(m_structure->coldDepthBegin());

            PointStatsMap pointStatsMap;
            Hierarchy localHierarchy(*m_bbox, *m_nodePool);
            Clipper clipper(*this, 0);

            branch->recurse(cold, [&](const Id& chunkId, std::size_t depth)
            {
                std::cout << "\nAdding " << chunkId << " " << depth << std::endl;

                auto compressed(
                    m_outEndpoint->getSubpathBinary(
                        m_structure->maybePrefix(chunkId) +
                        other.postfix(true)));

                const auto tail(Chunk::popTail(compressed));

                Cell::PooledStack cells(
                        Compression::decompress(
                            compressed,
                            tail.numPoints,
                            *m_pointPool));

                compressed.clear();

                insertHinted(
                    reserves,
                    std::move(cells),
                    pointStatsMap,
                    clipper,
                    localHierarchy,
                    chunkId,
                    depth,
                    depth + 1);
            });

            m_manifest->add(pointStatsMap);

            std::lock_guard<std::mutex> lock(m_mutex);
            m_hierarchy->merge(localHierarchy);
        });
    });

    m_pool->join();

    std::cout << "Rejected more: " << countReserves() << std::endl;

    m_hierarchy->setStep(Hierarchy::defaultStep);
}

void Builder::insertHinted(
        Reserves& reserves,
        Cell::PooledStack cells,
        PointStatsMap& pointStatsMap,
        Clipper& clipper,
        Hierarchy& localHierarchy,
        const Id& chunkId,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    /*
    std::cout << "Inserting: " << cells.size() << std::endl;

    Climber climber(*m_bbox, *m_structure, &localHierarchy);

    Origin origin(0);

    BinaryPointTable binaryTable(*m_schema);
    pdal::PointRef pointRef(binaryTable, 0);

    std::size_t rejects(0);

    while (!infoStack.empty())
    {
        PooledInfoNode info(infoStack.popOne());
        const Point& point(info->val().point());

        binaryTable.setPoint(info->val().data());
        origin = pointRef.getFieldAs<uint64_t>(m_originId);

        climber.reset();
        climber.magnifyTo(point, depthBegin);

        if (m_registry->addPoint(info, climber, clipper, depthEnd))
        {
            pointStatsMap[origin].addInsert();
        }
        else
        {
            ++rejects;

            if (m_structure->inRange(climber.depth() + 1))
            {
                climber.magnify(point);

                std::lock_guard<std::mutex> lock(m_mutex);
                reserves[climber.chunkId()].emplace_back(
                        climber,
                        std::move(info));
            }
            else
            {
                pointStatsMap[origin].addOverflow();
            }
        }
    }

    std::vector<InfoState>* infoStateList(nullptr);

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (reserves.count(chunkId)) infoStateList = &reserves.at(chunkId);
    }

    if (infoStateList)
    {
        for (auto& infoState : *infoStateList)
        {
            PooledInfoNode info(infoState.acquireInfoNode());

            binaryTable.setPoint(info->val().data());
            origin = pointRef.getFieldAs<uint64_t>(m_originId);

            Climber& reserveClimber(infoState.climber());

            if (m_registry->addPoint(info, reserveClimber, clipper, depthEnd))
            {
                pointStatsMap[origin].addInsert();
            }
            else
            {
                ++rejects;

                if (m_structure->inRange(reserveClimber.depth() + 1))
                {
                    reserveClimber.magnify(info->val().point());

                    std::lock_guard<std::mutex> lock(m_mutex);
                    reserves[reserveClimber.chunkId()].emplace_back(
                            reserveClimber,
                            std::move(info));
                }
                else
                {
                    pointStatsMap[origin].addOverflow();
                }
            }
        }

        std::lock_guard<std::mutex> lock(m_mutex);
        reserves.erase(chunkId);
    }

    std::cout << "Rejected: " << rejects << std::endl;
    */
}

void Builder::merge(Builder& other)
{
    if (!subset())
    {
        throw std::runtime_error("Cannot merge non-subset build");
    }

    if (m_srs.empty() && !other.srs().empty())
    {
        m_srs = other.srs();
    }

    m_registry->merge(other.registry());
    m_manifest->merge(other.manifest());
    m_hierarchy->merge(other.hierarchy());
}

Json::Value Builder::saveOwnProps() const
{
    Json::Value props;

    props["bboxConforming"] = m_bboxConforming->toJson();
    props["bbox"] = m_bbox->toJson();
    props["schema"] = m_schema->toJson();
    props["structure"] = m_structure->toJson();
    props["hierarchy"] = m_hierarchy->toJson(
            m_outEndpoint->getSubEndpoint("h"),
            postfix());

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

void Builder::loadProps(
        const OuterScope outerScope,
        Json::Value& props,
        const std::string pf)
{
    m_bboxConforming.reset(new BBox(props["bboxConforming"]));
    m_bbox.reset(new BBox(props["bbox"]));
    m_schema.reset(new Schema(props["schema"]));
    m_pointPool = outerScope.getPointPool(*m_schema);
    m_nodePool = outerScope.getNodePool();
    m_structure.reset(new Structure(props["structure"]));
    m_hierarchy.reset(
            new Hierarchy(
                *m_bbox,
                *m_nodePool,
                props["hierarchy"],
                m_outEndpoint->getSubEndpoint("h"),
                pf));

    if (props.isMember("subset"))
    {
        m_subset.reset(new Subset(*m_structure, *m_bbox, props["subset"]));
    }

    if (props.isMember("reprojection"))
    {
        m_reprojection.reset(new Reprojection(props["reprojection"]));
    }

    if (props.isMember("manifest"))
    {
        m_manifest.reset(new Manifest(props["manifest"]));
    }

    m_srs = props["srs"].asString();
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
        if (!m_outEndpoint->isRemote())
        {
            if (
                    !arbiter::fs::mkdirp(rootDir) ||
                    !arbiter::fs::mkdirp(rootDir + "h"))
            {
                throw std::runtime_error(
                        "Couldn't create local build directory");
            }
        }
    }
}

std::string Builder::postfix(const bool isColdChunk) const
{
    // Things we save, and their postfixing.
    //
    // Metadata files (main meta, ids, manifest):
    //      All postfixes applied.
    //
    // Base chunk:
    //      All postfixes applied.
    //
    // Other chunks:
    //      No subset postfixing.
    //      Split postfixing applied to splits except for the nominal split.
    //
    // Hierarchy:
    //      All postfixes applied.
    std::string pf;

    if (!isColdChunk && m_subset) pf += m_subset->postfix();
    if (const Manifest::Split* split = m_manifest->split())
    {
        if (!isColdChunk || split->begin()) pf += split->postfix();
    }

    return pf;
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

    m_manifest->unsplit();
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
const arbiter::Arbiter& Builder::arbiter() const { return *m_arbiter; }
Hierarchy& Builder::hierarchy()             { return *m_hierarchy; }

const Reprojection* Builder::reprojection() const
{
    return m_reprojection.get();
}

PointPool& Builder::pointPool() const { return *m_pointPool; }
std::shared_ptr<PointPool> Builder::sharedPointPool() const
{
    return m_pointPool;
}

Node::NodePool& Builder::nodePool() const { return *m_nodePool; }
std::shared_ptr<Node::NodePool> Builder::sharedNodePool() const
{
    return m_nodePool;
}

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

void Builder::traverse(
        const std::string output,
        const std::size_t threads,
        const double tileWidth,
        const TileFunction& f) const
{

    Tiler traverser(
            *this,
            m_arbiter->getEndpoint(output),
            threads,
            tileWidth);

    traverser.go(f);
}

void Builder::traverse(
        const std::size_t threads,
        const double tileWidth,
        const TileFunction& f,
        const Schema* schema) const
{
    Tiler traverser(*this, threads, tileWidth, schema);
    traverser.go(f);
}

} // namespace entwine


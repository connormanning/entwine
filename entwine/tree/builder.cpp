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
#include <entwine/tree/thread-pools.hpp>
// #include <entwine/tree/tiler.hpp>
// #include <entwine/tree/traverser.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/compression.hpp>
#include <entwine/util/executor.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

using namespace arbiter;

namespace
{
    const std::size_t sleepCount(65536 * 24);
}

Builder::Builder(
        const Metadata& metadata,
        const std::string outPath,
        const std::string tmpPath,
        const std::size_t totalThreads,
        const OuterScope outerScope)
    : m_arbiter(outerScope.getArbiter())
    , m_outEndpoint(makeUnique<Endpoint>(m_arbiter->getEndpoint(outPath)))
    , m_tmpEndpoint(makeUnique<Endpoint>(m_arbiter->getEndpoint(tmpPath)))
    , m_metadata(makeUnique<Metadata>(metadata))
    , m_mutex()
    , m_isContinuation(false)
    , m_threadPools(makeUnique<ThreadPools>(totalThreads))
    , m_executor(makeUnique<Executor>())
    , m_originId(m_metadata->schema().pdalLayout().findDim("Origin"))
    , m_origin(0)
    , m_end(0)
    , m_added(0)
    , m_pointPool(outerScope.getPointPool(m_metadata->schema()))
    , m_nodePool(outerScope.getNodePool())
    , m_hierarchy(makeUnique<Hierarchy>(*m_metadata))
    , m_registry(makeUnique<Registry>(*this))
{
    prepareEndpoints();
}

Builder::Builder(
        const std::string outPath,
        const std::string tmpPath,
        const std::size_t totalThreads,
        const std::size_t* subId,
        const OuterScope outerScope)
    : m_arbiter(outerScope.getArbiter())
    , m_outEndpoint(makeUnique<Endpoint>(m_arbiter->getEndpoint(outPath)))
    , m_tmpEndpoint(makeUnique<Endpoint>(m_arbiter->getEndpoint(tmpPath)))
    , m_metadata(makeUnique<Metadata>(m_arbiter->getEndpoint(outPath), subId))
    , m_mutex()
    , m_isContinuation(true)
    , m_threadPools(makeUnique<ThreadPools>(totalThreads))
    , m_executor(makeUnique<Executor>())
    , m_originId(m_metadata->schema().pdalLayout().findDim("Origin"))
    , m_origin(0)
    , m_end(0)
    , m_added(0)
    , m_pointPool(outerScope.getPointPool(m_metadata->schema()))
    , m_nodePool(outerScope.getNodePool())
    , m_hierarchy(makeUnique<Hierarchy>(*m_metadata))
    , m_registry(makeUnique<Registry>(*this, true))
{
    prepareEndpoints();
    m_hierarchy->awakenAll();
}

/*
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
    , m_workThreads(getWorkThreads(totalThreads))
    , m_clipThreads(getClipThreads(totalThreads))
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
    prepareEndpoints();
    load(outerScope, subsetId, splitBegin);
}
*/

std::unique_ptr<Builder> Builder::create(
        const std::string path,
        const std::size_t threads,
        const OuterScope os)
{
    std::unique_ptr<Builder> builder;
    const std::size_t zero(0);

    // Try a subset build.
    try { builder = makeUnique<Builder>(path, ".", threads, &zero, os); }
    catch (...) { }

    return builder;
}

std::unique_ptr<Builder> Builder::create(
        const std::string path,
        const std::size_t threads,
        const std::size_t subId,
        const OuterScope os)
{
    std::unique_ptr<Builder> builder;

    // Try a subset build.
    try { builder = makeUnique<Builder>(path, ".", threads, &subId, os); }
    catch (...) { }

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

    Manifest& manifest(m_metadata->manifest());
    m_end = manifest.size();

    if (const Manifest::Split* split = manifest.split())
    {
        m_origin = split->begin();
        m_end = split->end();
    }

    max = max ? std::min<std::size_t>(m_end, max) : m_end;

    while (keepGoing() && m_added < max)
    {
        FileInfo& info(manifest.get(m_origin));
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

        m_threadPools->workPool().add([this, origin, &info, path]()
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

            m_metadata->manifest().set(origin, status);
        });

        next();
    }

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
        m_metadata->manifest().set(m_origin, FileInfo::Status::Omitted);
        return false;
    }
    else if (const BBox* bbox = info.bbox())
    {
        if (!checkBounds(m_origin, *bbox, info.numPoints()))
        {
            m_metadata->manifest().set(m_origin, FileInfo::Status::Inserted);
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
    if (!m_metadata->bbox().overlaps(bbox))
    {
        const Subset* subset(m_metadata->subset());
        const bool primary(!subset || subset->primary());
        m_metadata->manifest().addOutOfBounds(origin, numPoints, primary);
        return false;
    }
    else if (const BBox* bboxSubset = m_metadata->bboxSubset())
    {
        if (!bboxSubset->overlaps(bbox)) return false;
    }

    return true;
}

bool Builder::insertPath(const Origin origin, FileInfo& info)
{
    auto localHandle(m_arbiter->getLocalHandle(info.path(), *m_tmpEndpoint));
    const std::string& localPath(localHandle->localPath());

    const Reprojection* reprojection(m_metadata->reprojection());

    // If we don't have an inferred bbox, check against the actual file.
    if (!info.bbox())
    {
        auto pre(m_executor->preview(localPath, reprojection));

        if (pre && !checkBounds(origin, pre->bbox, pre->numPoints))
        {
            return false;
        }
    }

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string& srs(m_metadata->srs());

        if (srs.empty())
        {
            if (reprojection)
            {
                // Don't construct the pdal::SpatialReference ourself, since
                // we need to use the Executors lock to do so.
                srs = m_executor->getSrsString(reprojection->out());
            }
            else
            {
                auto preview(m_executor->preview(localPath, nullptr));
                if (preview) srs = preview->srs;
            }

            if (srs.size()) std::cout << "Found an SRS" << std::endl;
        }
    }

    std::size_t inserted(0);

    Clipper clipper(*this, origin);
    Climber climber(*m_metadata, m_hierarchy.get());

    auto inserter([this, origin, &clipper, &climber, &inserted]
    (Cell::PooledStack cells)
    {
        inserted += cells.size();

        if (inserted > sleepCount)
        {
            inserted = 0;
            clipper.clip();
        }

        return insertData(std::move(cells), origin, clipper, climber);
    });

    PooledPointTable table(*m_pointPool, inserter, m_originId, origin);
    return m_executor->run(table, localPath, reprojection);
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

    const BBox& bboxConforming(m_metadata->bboxEpsilon());
    const BBox* bboxSubset(m_metadata->bboxSubset());
    const std::size_t baseDepthBegin(m_metadata->structure().baseDepthBegin());

    while (!cells.empty())
    {
        Cell::PooledNode cell(cells.popOne());
        const Point& point(cell->point());

        if (bboxConforming.contains(point))
        {
            if (!bboxSubset || bboxSubset->contains(point))
            {
                climber.reset();
                climber.magnifyTo(point, baseDepthBegin);

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

    if (origin != invalidOrigin) m_metadata->manifest().add(origin, pointStats);
    return rejected;
}

/*
void Builder::load(
        const OuterScope outerScope,
        const std::size_t clipThreads,
        const std::string pf)
{
    Json::Value meta;
    Json::Reader reader;

    {
        const std::string strIds(
                m_outEndpoint->getSubpath("entwine-ids" + pf));

        if (!reader.parse(strIds, meta["ids"], false)) error();
    }

    m_hierarchy.reset(
            new Hierarchy(
                *m_bbox,
                *m_nodePool,
                props["hierarchy"],
                m_outEndpoint->getSubEndpoint("h"),
                pf));

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
*/

void Builder::save()
{
    std::cout << "\tPushes complete - joining..." << std::endl;
    m_threadPools->join();
    std::cout << "\tJoined - saving..." << std::endl;

    m_metadata->save(*m_outEndpoint);
    m_registry->save();
    m_hierarchy->save(
            m_outEndpoint->getSubEndpoint("h"), m_metadata->postfix());
}

void Builder::unsplit(Builder& other)
{
    /*
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
    */
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
    if (!m_metadata->subset())
    {
        throw std::runtime_error("Cannot merge non-subset build");
    }

    m_registry->merge(*other.m_registry);
    m_metadata->merge(*other.m_metadata);
    m_hierarchy->merge(*other.m_hierarchy);
}

void Builder::prepareEndpoints()
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

void Builder::stop()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_end = std::min(m_end, m_origin + 1);
    std::cout << "Setting end at " << m_end << std::endl;
}

void Builder::makeWhole()
{
    m_metadata->makeWhole();
}

std::unique_ptr<Manifest::Split> Builder::takeWork()
{
    std::unique_ptr<Manifest::Split> split;

    std::lock_guard<std::mutex> lock(m_mutex);

    Manifest& manifest(m_metadata->manifest());

    const std::size_t remaining(m_end - m_origin);
    const double ratioRemaining(
            static_cast<double>(remaining) /
            static_cast<double>(manifest.size()));

    if (remaining > 2 && ratioRemaining > 0.05)
    {
        m_end = m_origin + remaining / 2;
        split = manifest.split(m_end);
    }

    return split;
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

const Metadata& Builder::metadata() const           { return *m_metadata; }
const Registry& Builder::registry() const           { return *m_registry; }
const Hierarchy& Builder::hierarchy() const         { return *m_hierarchy; }
const arbiter::Arbiter& Builder::arbiter() const    { return *m_arbiter; }

ThreadPools& Builder::threadPools() const { return *m_threadPools; }

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
    m_metadata->errors().push_back(file + ": " + error);
}

/*
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
*/

} // namespace entwine


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
#include <numeric>
#include <random>
#include <thread>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/third/splice-pool/splice-pool.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/tree/heuristics.hpp>
#include <entwine/tree/hierarchy-block.hpp>
#include <entwine/tree/registry.hpp>
#include <entwine/tree/thread-pools.hpp>
#include <entwine/tree/traverser.hpp>
#include <entwine/types/bounds.hpp>
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
    const std::size_t inputRetryLimit(8);
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
    , m_hierarchy(makeUnique<Hierarchy>(
                *m_metadata,
                *m_outEndpoint,
                m_outEndpoint.get(),
                false))
    , m_registry(makeUnique<Registry>(*this))
{
    prepareEndpoints();
}

Builder::Builder(
        const std::string outPath,
        const std::string tmpPath,
        const std::size_t totalThreads,
        const std::size_t* subId,
        const std::size_t* splId,
        const OuterScope outerScope)
    : m_arbiter(outerScope.getArbiter())
    , m_outEndpoint(makeUnique<Endpoint>(m_arbiter->getEndpoint(outPath)))
    , m_tmpEndpoint(makeUnique<Endpoint>(m_arbiter->getEndpoint(tmpPath)))
    , m_metadata(makeUnique<Metadata>(*m_outEndpoint, subId, splId))
    , m_mutex()
    , m_isContinuation(true)
    , m_threadPools(makeUnique<ThreadPools>(totalThreads))
    , m_executor(makeUnique<Executor>())
    , m_originId(m_metadata->schema().pdalLayout().findDim("Origin"))
    , m_origin(0)
    , m_end(0)
    , m_added(0)
    , m_pointPool(outerScope.getPointPool(m_metadata->schema()))
    , m_hierarchy(makeUnique<Hierarchy>(
                *m_metadata,
                *m_outEndpoint,
                m_outEndpoint.get(),
                true))
    , m_registry(makeUnique<Registry>(*this, true))
{
    prepareEndpoints();
}

std::unique_ptr<Builder> Builder::create(
        const std::string path,
        const std::size_t threads,
        OuterScope os)
{
    const std::size_t zero(0);

    // Try a subset build.
    try { return makeUnique<Builder>(path, ".", threads, &zero, nullptr, os); }
    catch (...) { }

    // Try a split build.
    try { return makeUnique<Builder>(path, ".", threads, nullptr, &zero,  os); }
    catch (...) { }

    // Try a subset and split build.
    try { return makeUnique<Builder>(path, ".", threads, &zero, &zero,  os); }
    catch (...) { }

    return std::unique_ptr<Builder>();
}

std::unique_ptr<Builder> Builder::create(
        const std::string path,
        const std::size_t threads,
        const std::size_t* subId,
        const std::size_t* splId,
        const OuterScope os)
{
    if (!subId && !splId) return Builder::create(path, threads, os);

    try { return makeUnique<Builder>(path, ".", threads, subId, splId, os); }
    catch (...) { }

    if (!subId) return std::unique_ptr<Builder>();

    const std::size_t zero(0);
    try { return makeUnique<Builder>(path, ".", threads, subId, &zero, os); }
    catch (...) { }

    return std::unique_ptr<Builder>();
}

Builder::~Builder()
{ }

void Builder::go(std::size_t max)
{
    m_hierarchy->awakenAll();

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
        std::cout <<
            " A: " << m_pointPool->cellPool().allocated() <<
            " C: " << Chunk::count() <<
            " H: " << HierarchyBlock::count() <<
            std::endl;

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

    std::cout << "\tPushes complete - joining..." << std::endl;
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
    else if (const Bounds* bounds = info.bounds())
    {
        if (!checkBounds(m_origin, *bounds, info.numPoints()))
        {
            m_metadata->manifest().set(m_origin, FileInfo::Status::Inserted);
            return false;
        }
    }

    return true;
}

bool Builder::checkBounds(
        const Origin origin,
        const Bounds& bounds,
        const std::size_t numPoints)
{
    if (!m_metadata->bounds().overlaps(bounds))
    {
        const Subset* subset(m_metadata->subset());
        const bool primary(!subset || subset->primary());
        m_metadata->manifest().addOutOfBounds(origin, numPoints, primary);
        return false;
    }
    else if (const Bounds* boundsSubset = m_metadata->boundsSubset())
    {
        if (!boundsSubset->overlaps(bounds)) return false;
    }

    return true;
}

bool Builder::insertPath(const Origin origin, FileInfo& info)
{
    const auto rawPath(info.path());
    std::size_t tries(0);
    std::unique_ptr<arbiter::fs::LocalHandle> localHandle;

    do
    {
        try
        {
            localHandle = m_arbiter->getLocalHandle(rawPath, *m_tmpEndpoint);
        }
        catch (const ArbiterError& e)
        {
            std::cout <<
                "Failed GET attempt of " << rawPath << ": " << e.what() <<
                std::endl;

            localHandle.reset();
            std::this_thread::sleep_for(std::chrono::seconds(tries));
        }
    }
    while (!localHandle && ++tries < inputRetryLimit);

    const std::string& localPath(localHandle->localPath());

    const Reprojection* reprojection(m_metadata->reprojection());

    // If we don't have an inferred bounds, check against the actual file.
    if (!info.bounds())
    {
        auto pre(m_executor->preview(localPath, reprojection));

        if (pre && !checkBounds(origin, pre->bounds, pre->numPoints))
        {
            return false;
        }
    }

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string& srs(m_metadata->format().srs());

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

        if (inserted > heuristics::sleepCount)
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

    const Bounds& boundsConforming(m_metadata->boundsEpsilon());
    const Bounds* boundsSubset(m_metadata->boundsSubset());
    const std::size_t baseDepthBegin(m_metadata->structure().baseDepthBegin());

    while (!cells.empty())
    {
        Cell::PooledNode cell(cells.popOne());
        const Point& point(cell->point());

        if (boundsConforming.contains(point))
        {
            if (!boundsSubset || boundsSubset->contains(point))
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

void Builder::save()
{
    save(*m_outEndpoint);
}

void Builder::save(const std::string to)
{
    save(m_arbiter->getEndpoint(to));
}

void Builder::save(const arbiter::Endpoint& ep)
{
    m_threadPools->join();

    std::cout << "Saving hierarchy..." << std::endl;
    m_hierarchy->save();

    std::cout << "Saving registry..." << std::endl;
    m_registry->save(*m_outEndpoint);

    std::cout << "Saving metadata..." << std::endl;
    m_metadata->save(*m_outEndpoint);
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

    if (remaining > 2 && ratioRemaining > 0.0025)
    {
        m_end = m_origin + remaining / 2;
        split = manifest.split(m_end);

        std::cout << "Setting end at " << m_end << std::endl;
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

const arbiter::Endpoint& Builder::outEndpoint() const { return *m_outEndpoint; }
const arbiter::Endpoint& Builder::tmpEndpoint() const { return *m_tmpEndpoint; }

void Builder::clip(
        const Id& index,
        const std::size_t chunkNum,
        const std::size_t id,
        const bool sync)
{
    m_registry->clip(index, chunkNum, id, sync);
}

void Builder::addError(const std::string& path, const std::string& error)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    const std::size_t lastSlash(path.find_last_of('/'));
    const std::string file(
            lastSlash != std::string::npos ? path.substr(lastSlash + 1) : path);
    m_metadata->errors().push_back(file + ": " + error);
}

void Builder::unsplit(Builder& other)
{
    if (m_threadPools->ratio() != 1.0) m_threadPools->setRatio(1.0);

    auto& manifest(m_metadata->manifest());
    auto& otherManifest(other.m_metadata->manifest());

    // We'll be bookkeeping these for ourselves as we unsplit.
    otherManifest.clearPointStats();

    if (!manifest.split() || !otherManifest.split())
    {
        throw std::runtime_error("Cannot unsplit a builder that wasn't split");
    }

    if (manifest.split()->end() != otherManifest.split()->begin())
    {
        throw std::runtime_error("Splits don't line up");
    }

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

    if (Chunk* otherBase = other.m_registry->cold().base())
    {
        std::cout << "Inserting base..." << std::endl;
        PointStatsMap pointStatsMap;
        Clipper clipper(*this, 0);

        insertHinted(
                reserves,
                otherBase->acquire(),
                pointStatsMap,
                clipper,
                Id(0),
                m_metadata->structure().baseDepthBegin(),
                m_metadata->structure().baseDepthEnd());

        m_metadata->manifest().add(pointStatsMap);
        std::cout << "\tBase inserted" << std::endl;
    }

    std::size_t n(0);
    Traverser traverser(*m_metadata, other.registry().cold().ids());
    traverser.tree([&](const Branch branch)
    {
        branch.recurse(
                m_threadPools->workPool(),
                [&](const Id chunkId, std::size_t depth)
        {
            const auto path(m_metadata->structure().maybePrefix(chunkId));
            const auto postfix(other.metadata().postfix(true));

            {
                std::lock_guard<std::mutex> lock(m_mutex);
                ++n;
                std::cout << "+" << chunkId << " @ " << depth << std::endl;
            }

            PointStatsMap pointStatsMap;
            Clipper clipper(*this, 0);

            std::unique_ptr<std::vector<char>> data(
                    makeUnique<std::vector<char>>(
                        m_outEndpoint->getBinary(path + postfix)));

            Unpacker unpacker(m_metadata->format().unpack(std::move(data)));

            Cell::PooledStack cells(unpacker.acquireCells(*m_pointPool));

            insertHinted(
                reserves,
                std::move(cells),
                pointStatsMap,
                clipper,
                chunkId,
                depth,
                depth + 1);

            clipper.clip(chunkId);

            m_metadata->manifest().add(pointStatsMap);

            {
                std::lock_guard<std::mutex> lock(m_mutex);
                std::cout << "\t~" << chunkId << std::endl;
            }
        });
    });

    std::cout << "Recursed " << n << " chunks." << std::endl;

    m_threadPools->cycle();

    // Insert any fall-through points that have fallen past all pre-existing
    // chunks.
    PointStatsMap pointStatsMap;

    if (reserves.size())
    {
        std::cout <<
            "Inserting pre-existing fall-throughs: " << countReserves() <<
            " from " << reserves.size() << " chunks." << std::endl;
        Clipper clipper(*this, 0);

        // Cache the IDs in reserves, which will be modified as we insert.
        std::set<Id> leftovers;
        for (const auto& p : reserves) leftovers.insert(p.first);

        for (const Id& id : leftovers)
        {
            m_threadPools->workPool().add([&, id]()
            {
                std::cout << "Adding " << id << std::endl;
                Cell::PooledStack empty(m_pointPool->cellPool());
                insertHinted(
                        reserves,
                        std::move(empty),
                        pointStatsMap,
                        clipper,
                        id,
                        std::numeric_limits<std::size_t>::max(),
                        std::numeric_limits<std::size_t>::max());
                std::cout << "\tAdded " << id << std::endl;
            });
        }

        m_threadPools->cycle();
    }

    manifest.add(pointStatsMap);
    manifest.split(manifest.split()->begin(), otherManifest.split()->end());

    if (manifest.split()->end() == manifest.size())
    {
        manifest.unsplit();
    }

    manifest.merge(otherManifest);

    if (countReserves())
    {
        std::cout << "Final fall-throughs: " << countReserves() << std::endl;
    }
}

void Builder::insertHinted(
        Reserves& reserves,
        Cell::PooledStack cells,
        PointStatsMap& pointStatsMap,
        Clipper& clipper,
        const Id& chunkId,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    std::vector<Origin> origins;

    BinaryPointTable table(m_metadata->schema());
    pdal::PointRef pointRef(table, 0);

    std::size_t rejects(0);

    auto insert([&](
                Cell::PooledNode& cell,
                Climber& climber,
                const std::vector<Origin>* natives)
    {
        // If native origins aren't given to us from above, then we must derive
        // them from the points in this cell to track the state.
        if (!natives)
        {
            origins.clear();
            for (const auto d : *cell)
            {
                table.setPoint(d);
                origins.push_back(pointRef.getFieldAs<uint64_t>(m_originId));
            }

            natives = &origins;
        }

        if (m_registry->addPoint(cell, climber, clipper, depthEnd))
        {
            for (const auto o : *natives) pointStatsMap[o].addInsert();
        }
        else
        {
            ++rejects;

            if (m_metadata->structure().inRange(climber.depth() + 1))
            {
                climber.magnify(cell->point());

                std::lock_guard<std::mutex> lock(m_mutex);
                reserves[climber.chunkId()].emplace_back(
                        climber,
                        std::move(cell),
                        *natives);
            }
            else
            {
                for (const auto o : *natives) pointStatsMap[o].addOverflow();
            }
        }
    });

    // First, insert points from this exact chunk, indexed by the other Builder.
    // Points may fall through here, and if so, save their Climber state to
    // insert into their chunk at the next depth.
    {
        Climber climber(*m_metadata, m_hierarchy.get(), false);

        while (!cells.empty())
        {
            Cell::PooledNode cell(cells.popOne());
            const Point& point(cell->point());

            while (cells.head() && (*cells.head())->point() == point)
            {
                cell->push(cells.popOne(), m_metadata->schema().pointSize());
            }

            climber.reset();
            climber.magnifyTo(point, depthBegin);

            insert(cell, climber, nullptr);
        }
    }

    Reserves::value_type::second_type* cellStateList(nullptr);

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (reserves.count(chunkId)) cellStateList = &reserves.at(chunkId);
    }

    // Now, we may have points from that have fallen through to this chunk from
    // prior merge-insertions.  Insert them now.  Since they may fall through
    // again, save their state if so.
    if (cellStateList)
    {
        for (auto& cellState : *cellStateList)
        {
            Cell::PooledNode cell(cellState.acquireCellNode());
            insert(cell, cellState.climber(), &cellState.origins());
        }

        std::lock_guard<std::mutex> lock(m_mutex);
        reserves.erase(chunkId);
    }
}

} // namespace entwine


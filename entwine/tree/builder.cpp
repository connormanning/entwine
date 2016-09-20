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
#include <entwine/tree/sequence.hpp>
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
    , m_sequence(makeUnique<Sequence>(*this))
    , m_originId(m_metadata->schema().pdalLayout().findDim("Origin"))
    , m_pointPool(outerScope.getPointPool(m_metadata->schema()))
    , m_hierarchyPool(outerScope.getHierarchyPool(heuristics::poolBlockSize))
    , m_hierarchy(makeUnique<Hierarchy>(
                *m_hierarchyPool,
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
        const OuterScope outerScope)
    : m_arbiter(outerScope.getArbiter())
    , m_outEndpoint(makeUnique<Endpoint>(m_arbiter->getEndpoint(outPath)))
    , m_tmpEndpoint(makeUnique<Endpoint>(m_arbiter->getEndpoint(tmpPath)))
    , m_metadata(makeUnique<Metadata>(*m_outEndpoint, subId))
    , m_mutex()
    , m_isContinuation(true)
    , m_threadPools(makeUnique<ThreadPools>(totalThreads))
    , m_executor(makeUnique<Executor>())
    , m_sequence(makeUnique<Sequence>(*this))
    , m_originId(m_metadata->schema().pdalLayout().findDim("Origin"))
    , m_pointPool(outerScope.getPointPool(m_metadata->schema()))
    , m_hierarchyPool(outerScope.getHierarchyPool(heuristics::poolBlockSize))
    , m_hierarchy(makeUnique<Hierarchy>(
                *m_hierarchyPool,
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
        const std::size_t* subsetId,
        OuterScope os)
{
    // Try the passed-in build.
    try { return makeUnique<Builder>(path, ".", threads, subsetId, os); }
    catch (...) { }

    if (!subsetId)
    {
        // Try the subset-zero build.
        const std::size_t zero(0);
        try { return makeUnique<Builder>(path, ".", threads, &zero, os); }
        catch (...) { }
    }

    return std::unique_ptr<Builder>();
}

Builder::~Builder()
{ }

void Builder::go(std::size_t max)
{
    m_hierarchy->awakenAll(m_threadPools->workPool());

    if (!m_tmpEndpoint)
    {
        throw std::runtime_error("Cannot add to read-only builder");
    }

    while (auto o = m_sequence->next(max))
    {
        const Origin origin(*o);
        FileInfo& info(m_metadata->manifest().get(origin));
        const auto path(info.path());

        std::cout << "Adding " << origin << " - " << path << std::endl;
        std::cout <<
            " A: " << m_pointPool->cellPool().allocated() <<
            " C: " << Chunk::count() <<
            " H: " << HierarchyBlock::count() <<
            std::endl;

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
    }

    std::cout << "\tPushes complete - joining..." << std::endl;
    save();
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
    const Transformation* transformation(m_metadata->transformation());

    // If we don't have an inferred bounds, check against the actual file.
    if (!info.bounds())
    {
        if (auto pre = m_executor->preview(localPath, reprojection))
        {
            if (!m_sequence->checkBounds(origin, pre->bounds, pre->numPoints))
            {
                return false;
            }
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
    return m_executor->run(table, localPath, reprojection, transformation);
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
    m_threadPools->cycle();

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

    if (!m_threadPools->clipPool().joined())
    {
        m_threadPools->workPool().resize(m_threadPools->size());
        m_threadPools->clipPool().join();
    }

    m_registry->merge(*other.m_registry);
    m_metadata->merge(*other.m_metadata);
    m_hierarchy->merge(*other.m_hierarchy, m_threadPools->workPool());
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
            if (!arbiter::fs::mkdirp(rootDir))
            {
                throw std::runtime_error("Couldn't create " + rootDir);
            }

            if (!arbiter::fs::mkdirp(rootDir + "h"))
            {
                throw std::runtime_error("Couldn't create " + rootDir + "h");
            }

            if (
                    m_metadata->cesiumSettings() &&
                    !arbiter::fs::mkdirp(rootDir + "cesium"))
            {
                throw std::runtime_error(
                        "Couldn't create " + rootDir + "cesium");
            }
        }
    }
}

void Builder::makeWhole() { m_metadata->makeWhole(); }

const Metadata& Builder::metadata() const           { return *m_metadata; }
const Registry& Builder::registry() const           { return *m_registry; }
const Hierarchy& Builder::hierarchy() const         { return *m_hierarchy; }
const arbiter::Arbiter& Builder::arbiter() const    { return *m_arbiter; }

Sequence& Builder::sequence() { return *m_sequence; }
const Sequence& Builder::sequence() const { return *m_sequence; }

ThreadPools& Builder::threadPools() const { return *m_threadPools; }

PointPool& Builder::pointPool() const { return *m_pointPool; }
std::shared_ptr<PointPool> Builder::sharedPointPool() const
{
    return m_pointPool;
}

std::shared_ptr<HierarchyCell::Pool> Builder::sharedHierarchyPool() const
{
    return m_hierarchyPool;
}

const arbiter::Endpoint& Builder::outEndpoint() const { return *m_outEndpoint; }
const arbiter::Endpoint& Builder::tmpEndpoint() const { return *m_tmpEndpoint; }

Executor& Builder::executor() { return *m_executor; }
std::mutex& Builder::mutex() { return m_mutex; }

void Builder::append(const Manifest& other)
{
    m_metadata->manifest().append(other);
    m_sequence = makeUnique<Sequence>(*this);
}

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

} // namespace entwine


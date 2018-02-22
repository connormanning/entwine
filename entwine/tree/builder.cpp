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
#include <entwine/tree/new-climber.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/tree/new-clipper.hpp>
#include <entwine/tree/heuristics.hpp>
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
#include <entwine/util/json.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

using namespace arbiter;

namespace
{
    const std::size_t inputRetryLimit(16);
}

Builder::Builder(
        const Metadata& metadata,
        const std::string outPath,
        const std::string tmpPath,
        const std::size_t workThreads,
        const std::size_t clipThreads,
        const OuterScope outerScope)
    : m_arbiter(outerScope.getArbiter())
    , m_out(makeUnique<Endpoint>(m_arbiter->getEndpoint(outPath)))
    , m_tmp(makeUnique<Endpoint>(m_arbiter->getEndpoint(tmpPath)))
    , m_threadPools(makeUnique<ThreadPools>(workThreads, clipThreads))
    , m_metadata(([this, &metadata]()
    {
        auto m(clone(metadata));
        m->manifest().awakenAll(m_threadPools->clipPool());
        return m;
    })())
    , m_isContinuation(false)
    , m_pointPool(
            outerScope.getPointPool(m_metadata->schema(), m_metadata->delta()))
    , m_sequence(makeUnique<Sequence>(*this))
    , m_registry(makeUnique<Registry>(
                *m_metadata,
                outEndpoint(),
                tmpEndpoint(),
                *m_pointPool))
    , m_start(now())
{
    prepareEndpoints();
}

/*
Builder::Builder(
        const std::string outPath,
        const std::string tmpPath,
        const std::size_t workThreads,
        const std::size_t clipThreads,
        const std::size_t* subsetId,
        const OuterScope outerScope)
    : m_arbiter(outerScope.getArbiter())
    , m_out(makeUnique<Endpoint>(m_arbiter->getEndpoint(outPath)))
    , m_tmp(makeUnique<Endpoint>(m_arbiter->getEndpoint(tmpPath)))
    , m_threadPools(makeUnique<ThreadPools>(workThreads, clipThreads))
    , m_metadata(Metadata::create(*m_out, subsetId))
    , m_isContinuation(true)
    , m_pointPool(
            outerScope.getPointPool(m_metadata->schema(), m_metadata->delta()))
    , m_sequence(makeUnique<Sequence>(*this))
    , m_registry(makeUnique<Registry>(
                *m_metadata,
                outEndpoint(),
                tmpEndpoint(),
                *m_pointPool,
                exists()))
    , m_start(now())
{
    if (m_metadata->manifestPtr())
    {
        m_metadata->manifest().awakenAll(m_threadPools->clipPool());
    }
    prepareEndpoints();
}
*/

std::unique_ptr<Builder> Builder::tryCreateExisting(
        const std::string out,
        const std::string tmp,
        const std::size_t works,
        const std::size_t clips,
        const std::size_t* subsetId,
        OuterScope os)
{
    /*
    const std::string postfix(Subset::postfix(subsetId));

    if (os.getArbiter()->getEndpoint(out).tryGetSize("entwine" + postfix))
    {
        return makeUnique<Builder>(out, tmp, works, clips, subsetId, os);
    }
    */

    return std::unique_ptr<Builder>();
}

Builder::~Builder()
{ }

void Builder::go(std::size_t max)
{
    bool done(false);
    const auto& manifest(m_metadata->manifest());

    const std::size_t alreadyInserted(manifest.pointStats().inserts());

    Pool p(2);
    p.add([this, max, &done]()
    {
        doRun(max);
        done = true;
    });

    p.add([this, &done, &manifest, alreadyInserted]()
    {
        using ms = std::chrono::milliseconds;
        const std::size_t interval(10);

        while (!done)
        {
            const auto t(since<ms>(m_start));
            std::this_thread::sleep_for(ms(1000 - t % 1000));
            const auto s(since<std::chrono::seconds>(m_start));

            if (s % interval == 0)
            {
                const double inserts(
                        manifest.pointStats().inserts() - alreadyInserted);

                const double progress(
                        (manifest.pointStats().inserts() + alreadyInserted) /
                        (double)(m_metadata->structure().numPointsHint()));

                const auto& d(pointPool().dataPool());

                const std::size_t used(
                        100.0 - 100.0 * d.available() / (double)d.allocated());

                std::cout <<
                    " T: " << commify(s) << "s" <<
                    " P: " << commify(inserts * 3600.0 / s / 1000000.0) <<
                        "M/h" <<
                    " A: " << commify(d.allocated()) <<
                    " U: " << used << "%"  <<
                    " I: " << commify(inserts) <<
                    " P: " << std::round(progress * 100.0) << "%" <<
                    std::endl;
            }
        }
    });

    p.join();
}

void Builder::doRun(const std::size_t max)
{
    if (!m_tmp)
    {
        throw std::runtime_error("Cannot add to read-only builder");
    }

    while (auto o = m_sequence->next(max))
    {
        const Origin origin(*o);
        FileInfo& info(m_metadata->manifest().get(origin));
        const auto path(info.path());

        if (verbose())
        {
            std::cout << "Adding " << origin << " - " << path << std::endl;
        }

        m_threadPools->workPool().add([this, origin, &info, path]()
        {
            FileInfo::Status status(FileInfo::Status::Inserted);
            std::string message;

            try
            {
                insertPath(origin, info);
            }
            catch (const std::exception& e)
            {
                if (verbose())
                {
                    std::cout << "During " << path << ": " << e.what() <<
                        std::endl;
                }

                status = FileInfo::Status::Error;
                message = e.what();
            }
            catch (...)
            {
                if (verbose())
                {
                    std::cout << "Unknown error during " << path << std::endl;
                }

                status = FileInfo::Status::Error;
                message = "Unknown error";
            }

            m_metadata->manifest().set(origin, status, message);
            if (verbose()) std::cout << "\tDone " << origin << std::endl;
        });
    }

    if (verbose())
    {
        std::cout << "\tPushes complete - joining..." << std::endl;
    }

    save();
}

void Builder::insertPath(const Origin origin, FileInfo& info)
{
    const std::string rawPath(info.path());
    std::size_t tries(0);
    std::unique_ptr<arbiter::fs::LocalHandle> localHandle;

    do
    {
        if (tries) std::this_thread::sleep_for(std::chrono::seconds(tries));

        try
        {
            localHandle = m_arbiter->getLocalHandle(rawPath, *m_tmp);
        }
        catch (const std::exception& e)
        {
            if (verbose())
            {
                std::cout <<
                    "Failed GET " << tries << " of " << rawPath << ": " <<
                    e.what() << std::endl;
            }
        }
        catch (...)
        {
            if (verbose())
            {
                std::cout <<
                    "Failed GET " << tries << " of " << rawPath << ": " <<
                    "unknown error" << std::endl;
            }
        }
    }
    while (!localHandle && ++tries < inputRetryLimit);

    if (!localHandle) throw std::runtime_error("No local handle: " + rawPath);

    const std::string& localPath(localHandle->localPath());

    const Reprojection* reprojection(m_metadata->reprojection());
    const Transformation* transformation(m_metadata->transformation());

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string& srs(m_metadata->srs());

        if (srs.empty())
        {
            auto preview(Executor::get().preview(localPath, nullptr));
            if (preview) srs = preview->srs;

            if (verbose() && srs.size())
            {
                std::cout << "Found an SRS" << std::endl;
            }
        }
    }

    std::size_t inserted(0);

    NewClipper clipper(*m_registry, origin);
    NewClimber climber(*m_metadata, origin);

    auto inserter([this, origin, &clipper, &climber, &inserted]
    (Cell::PooledStack cells)
    {
        inserted += cells.size();

        if (inserted > heuristics::sleepCount)
        {
            inserted = 0;
            const float available(m_pointPool->dataPool().available());
            const float allocated(m_pointPool->dataPool().allocated());
            // TODO Enable clip.
            // if (available / allocated < 0.5) clipper.clip();
        }

        return insertData(std::move(cells), origin, clipper, climber);
    });

    std::unique_ptr<PooledPointTable> table(
            PooledPointTable::create(
                *m_pointPool,
                inserter,
                m_metadata->delta(),
                origin));

    if (!Executor::get().run(
                *table,
                localPath,
                reprojection,
                transformation,
                m_metadata->preserveSpatial()))
    {
        throw std::runtime_error("Failed to execute: " + rawPath);
    }
}

Cells Builder::insertData(
        Cells cells,
        const Origin origin,
        NewClipper& clipper,
        NewClimber& climber)
{
    PointStats pointStats;
    Cells rejected(m_pointPool->cellPool());

    auto reject([&rejected](Cell::PooledNode& cell)
    {
        rejected.push(std::move(cell));
    });

    const Bounds& boundsConforming(m_metadata->boundsScaledEpsilon());
    const auto boundsSubset(nullptr); // m_metadata->boundsScaledSubset());

    while (!cells.empty())
    {
        Cell::PooledNode cell(cells.popOne());
        const Point& point(cell->point());

        if (boundsConforming.contains(point))
        {
            if (!boundsSubset) //  || boundsSubset->contains(point))
            {
                climber.init(point);

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
    save(*m_out);
}

void Builder::save(const std::string to)
{
    save(m_arbiter->getEndpoint(to));
}

void Builder::save(const arbiter::Endpoint& ep)
{
    m_threadPools->cycle();

    if (verbose()) std::cout << "Saving registry..." << std::endl;
    m_registry->save(*m_out);

    if (verbose()) std::cout << "Saving metadata..." << std::endl;
    m_metadata->save(*m_out);
}

void Builder::merge(Builder& other)
{
    /*
    if (!m_metadata->subset())
    {
        throw std::runtime_error("Cannot merge non-subset build");
    }
    */

    if (m_threadPools->clipPool().running())
    {
        m_threadPools->workPool().resize(m_threadPools->size());
        m_threadPools->clipPool().join();
    }

    m_registry->merge(*other.m_registry);
    if (other.exists())
    {
        m_metadata->merge(*other.m_metadata);
    }
}

void Builder::prepareEndpoints()
{
    if (m_tmp)
    {
        if (m_tmp->isRemote())
        {
            throw std::runtime_error("Tmp path must be local");
        }

        if (!arbiter::fs::mkdirp(m_tmp->root()))
        {
            throw std::runtime_error("Couldn't create tmp directory");
        }

        const std::string rootDir(m_out->root());
        if (!m_out->isRemote())
        {
            if (!arbiter::fs::mkdirp(rootDir))
            {
                throw std::runtime_error("Couldn't create " + rootDir);
            }

            if (!arbiter::fs::mkdirp(rootDir + "h"))
            {
                throw std::runtime_error("Couldn't create " + rootDir + "h");
            }

            if (!arbiter::fs::mkdirp(rootDir + "laz"))
            {
                throw std::runtime_error("Couldn't create " + rootDir + "h");
            }
        }
    }
}

void Builder::unbump() { m_metadata->unbump(); }
void Builder::makeWhole() { m_metadata->makeWhole(); }

const Metadata& Builder::metadata() const           { return *m_metadata; }
const Registry& Builder::registry() const           { return *m_registry; }
const arbiter::Arbiter& Builder::arbiter() const    { return *m_arbiter; }
arbiter::Arbiter& Builder::arbiter() { return *m_arbiter; }

Sequence& Builder::sequence() { return *m_sequence; }
const Sequence& Builder::sequence() const { return *m_sequence; }

ThreadPools& Builder::threadPools() const { return *m_threadPools; }

PointPool& Builder::pointPool() const { return *m_pointPool; }
std::shared_ptr<PointPool> Builder::sharedPointPool() const
{
    return m_pointPool;
}

const arbiter::Endpoint& Builder::outEndpoint() const { return *m_out; }
const arbiter::Endpoint& Builder::tmpEndpoint() const { return *m_tmp; }

std::mutex& Builder::mutex() { return m_mutex; }

void Builder::append(const FileInfoList& fileInfo)
{
    m_metadata->manifest().append(fileInfo);
    m_sequence = makeUnique<Sequence>(*this);
}

void Builder::clip(
        const Id& index,
        const std::size_t chunkNum,
        const std::size_t id,
        const bool sync)
{
    // m_registry->clip(index, chunkNum, id, sync);
}

void Builder::clip(const uint64_t d, const uint64_t x, const uint64_t y)
{
    // m_registry->clip(d, x, y);
}

} // namespace entwine


/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/builder/builder.hpp>

#include <chrono>
#include <limits>
#include <numeric>
#include <random>
#include <thread>

#include <entwine/builder/clipper.hpp>
#include <entwine/builder/heuristics.hpp>
#include <entwine/builder/registry.hpp>
#include <entwine/builder/sequence.hpp>
#include <entwine/builder/thread-pools.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/third/splice-pool/splice-pool.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/file-info.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
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
    std::size_t reawakened(0);
}

Builder::Builder(const Config& config, OuterScope os)
    : m_config(entwine::merge(
                Config::defaults(),
                Config::defaultBuildParams(),
                config.prepare().json()))
    , m_arbiter(os.getArbiter(m_config["arbiter"]))
    , m_out(makeUnique<Endpoint>(m_arbiter->getEndpoint(m_config.output())))
    , m_tmp(makeUnique<Endpoint>(m_arbiter->getEndpoint(m_config.tmp())))
    , m_threadPools(
            makeUnique<ThreadPools>(
                m_config.workThreads(),
                m_config.clipThreads()))
    , m_isContinuation(m_config.isContinuation())
    , m_sleepCount(m_config.sleepCount())
    , m_metadata(m_isContinuation ?
            makeUnique<Metadata>(*m_out, m_config) :
            makeUnique<Metadata>(m_config))
    , m_pointPool(os.getPointPool(m_metadata->schema(), m_metadata->delta()))
    , m_registry(makeUnique<Registry>(
                *m_metadata,
                *m_out,
                *m_tmp,
                *m_pointPool,
                *m_threadPools,
                m_isContinuation))
    , m_sequence(makeUnique<Sequence>(*m_metadata, m_mutex))
    , m_verbose(m_config.verbose())
    , m_start(now())
    , m_reset(now())
    , m_resetFiles(m_config["resetFiles"].asUInt64())
{
    prepareEndpoints();
}

Builder::~Builder()
{ }

void Builder::go(std::size_t max)
{
    m_start = now();
    m_reset = m_start;

    bool done(false);
    const auto& files(m_metadata->files());

    const std::size_t alreadyInserted(files.pointStats().inserts());

    Pool p(2);
    p.add([this, max, &done]()
    {
        doRun(max);
        done = true;
    });

    p.add([this, &done, &files, alreadyInserted]()
    {
        using ms = std::chrono::milliseconds;
        const std::size_t interval(10);
        std::size_t last(0);

        const double totalPoints(files.totalPoints());

        while (!done)
        {
            const auto t(since<ms>(m_start));
            std::this_thread::sleep_for(ms(1000 - t % 1000));
            const auto s(since<std::chrono::seconds>(m_start));

            if (s % interval == 0)
            {
                const double inserts(
                        files.pointStats().inserts() - alreadyInserted);

                const double progress(
                        (files.pointStats().inserts() + alreadyInserted) /
                        totalPoints);

                const auto& d(pointPool().dataPool());

                const std::size_t used(
                        100.0 - 100.0 * d.available() / (double)d.allocated());

                const auto info(ReffedChunk::latchInfo());
                reawakened += info.read;

                if (verbose())
                {
                    std::cout <<
                        " T: " << commify(s) << "s" <<
                        " R: " << commify(inserts * 3600.0 / s / 1000000.0) <<
                            "(" << commify((inserts - last) *
                                        3600.0 / 10.0 / 1000000.0) << ")" <<
                            "M/h" <<
                        " A: " << commify(d.allocated()) <<
                        " U: " << used << "%"  <<
                        " I: " << commify(inserts) <<
                        " P: " << std::round(progress * 100.0) << "%" <<
                        " W: " << info.written <<
                        " R: " << info.read <<
                        std::endl;
                }

                last = inserts;
            }
        }
    });

    p.join();
}

void Builder::cycle()
{
    if (verbose()) std::cout << "\tCycling memory pool" << std::endl;
    m_threadPools->cycle();
    m_pointPool->clear();
    m_reset = now();
    if (verbose()) std::cout << "\tCycled" << std::endl;
}

void Builder::doRun(const std::size_t max)
{
    if (!m_tmp)
    {
        throw std::runtime_error("Cannot add to read-only builder");
    }

    while (auto o = m_sequence->next(max))
    {
        if (
                (m_resetFiles && m_sequence->added() > m_resetFiles &&
                     (m_sequence->added() - 1) % m_resetFiles == 0) ||
                since<std::chrono::minutes>(m_reset) >= m_resetMinutes)
        {
            cycle();
        }

        const Origin origin(*o);
        FileInfo& info(m_metadata->mutableFiles().get(origin));
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

            m_metadata->mutableFiles().set(origin, status, message);
            if (verbose()) std::cout << "\tDone " << origin << std::endl;

            m_registry->purge();
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

    Clipper clipper(*m_registry, origin);

    auto inserter([this, &clipper, &inserted]
    (Cell::PooledStack cells)
    {
        inserted += cells.size();

        if (inserted > m_sleepCount)
        {
            inserted = 0;
            const float available(m_pointPool->dataPool().available());
            const float allocated(m_pointPool->dataPool().allocated());
            if (available / allocated < 0.5) clipper.clip();
        }

        return insertData(std::move(cells), clipper);
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
                transformation))
    {
        throw std::runtime_error("Failed to execute: " + rawPath);
    }
}

Cells Builder::insertData(Cells cells, Clipper& clipper)
{
    PointStats pointStats;
    Cells rejected(m_pointPool->cellPool());

    auto reject([&rejected](Cell::PooledNode& cell)
    {
        rejected.push(std::move(cell));
    });

    const Bounds& boundsConforming(m_metadata->boundsScaledConforming());
    const Bounds* boundsSubset(m_metadata->boundsScaledSubset());

    Key key(*m_metadata);

    while (!cells.empty())
    {
        Cell::PooledNode cell(cells.popOne());
        const Point& point(cell->point());

        if (boundsConforming.contains(point))
        {
            if (!boundsSubset || boundsSubset->contains(point))
            {
                key.init(point);

                if (m_registry->addPoint(cell, key, clipper))
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

    if (clipper.origin() != invalidOrigin)
    {
        m_metadata->mutableFiles().add(clipper.origin(), pointStats);
    }

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
    m_threadPools->join();
    m_threadPools->workPool().resize(m_threadPools->size());
    m_threadPools->go();
    m_pointPool->clear();

    if (verbose()) std::cout << "Reawakened: " << reawakened << std::endl;

    const auto& h(m_registry->hierarchy());
    if (
            !m_metadata->subset() &&
            !m_metadata->hierarchyStep() &&
            h.map().size() > heuristics::maxHierarchyNodesPerFile)
    {
        const auto analysis(h.analyze(*m_metadata));
        for (const auto& a : analysis) a.summarize();
        const auto& chosen(*analysis.begin());

        if (verbose())
        {
            std::cout << "Setting hierarchy step: " << chosen.step << std::endl;
        }
        m_metadata->setHierarchyStep(chosen.step);
    }

    if (verbose()) std::cout << "Saving registry..." << std::endl;
    m_registry->save(*m_out);

    if (verbose()) std::cout << "Saving metadata..." << std::endl;
    m_metadata->save(*m_out);
}

void Builder::merge(Builder& other, Clipper& clipper)
{
    m_registry->merge(*other.m_registry, clipper);
    m_metadata->merge(*other.m_metadata);
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
                throw std::runtime_error("Couldn't create hierarchy directory");
            }
        }
    }
}

void Builder::makeWhole() { m_metadata->makeWhole(); }

const Metadata& Builder::metadata() const           { return *m_metadata; }
const Registry& Builder::registry() const           { return *m_registry; }
const arbiter::Arbiter& Builder::arbiter() const    { return *m_arbiter; }
arbiter::Arbiter& Builder::arbiter() { return *m_arbiter; }
Registry& Builder::registry() { return *m_registry; }

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

} // namespace entwine


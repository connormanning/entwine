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
#include <entwine/types/bounds.hpp>
#include <entwine/types/file-info.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/scale-offset.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/executor.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    const std::size_t inputRetryLimit(16);
    std::size_t reawakened(0);

    std::size_t workThreads(const Metadata& m, const Config& c)
    {
        // Limit worker threads to the number of files.
        const std::size_t baseWorkThreads(c.workThreads());
        return std::min(baseWorkThreads, m.files().size());
    }

    std::size_t clipThreads(const Metadata& m, const Config& c)
    {
        const std::size_t baseWorkThreads(c.workThreads());
        const std::size_t baseClipThreads(c.clipThreads());

        // Actual work threads is <= base work threads.
        const std::size_t actualWorkThreads(workThreads(m, c));
        const std::size_t stolenWorkThreads(
                baseWorkThreads - actualWorkThreads);

        return baseClipThreads + stolenWorkThreads;
    }
}

Builder::Builder(const Config& config, std::shared_ptr<arbiter::Arbiter> a)
    : m_config(config.prepareForBuild())
    , m_interval(m_config.progressInterval())
    , m_arbiter(a ? a : std::make_shared<arbiter::Arbiter>(m_config.arbiter()))
    , m_out(makeUnique<arbiter::Endpoint>(
                m_arbiter->getEndpoint(m_config.output())))
    , m_tmp(makeUnique<arbiter::Endpoint>(
                m_arbiter->getEndpoint(m_config.tmp())))
    , m_isContinuation(m_config.isContinuation())
    , m_sleepCount(m_config.sleepCount())
    , m_metadata(m_isContinuation ?
            makeUnique<Metadata>(*m_out, m_config) :
            makeUnique<Metadata>(m_config))
    , m_threadPools(
            makeUnique<ThreadPools>(
                workThreads(*m_metadata, m_config),
                clipThreads(*m_metadata, m_config)))
    , m_registry(makeUnique<Registry>(
                *m_metadata,
                *m_out,
                *m_tmp,
                *m_threadPools,
                m_isContinuation))
    , m_sequence(makeUnique<Sequence>(*m_metadata, m_mutex))
    , m_verbose(m_config.verbose())
    , m_start(now())
{
    prepareEndpoints();
}

Builder::~Builder()
{ }

void Builder::go(std::size_t max)
{
    m_start = now();

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
        if (!m_interval) return;

        using ms = std::chrono::milliseconds;
        uint64_t lastInserts(0);
        int64_t lastProgress(0);

        const double totalPoints(files.totalPoints());
        const double megsPerHour(3600.0 / 1000000.0);

        while (!done)
        {
            const auto t(since<ms>(m_start));
            std::this_thread::sleep_for(ms(1000 - t % 1000));
            const auto s(since<std::chrono::seconds>(m_start));

            if (s != lastProgress && s % m_interval == 0)
            {
                lastProgress = s;

                const double inserts(
                        files.pointStats().inserts() - alreadyInserted);

                const double progress((inserts + alreadyInserted) / totalPoints);

                const ChunkCache::Info info(ChunkCache::latchInfo());
                reawakened += info.read;

                if (verbose())
                {
                    const uint64_t totalPace(
                            inserts /
                            static_cast<double>(s) *
                            megsPerHour);

                    const uint64_t lastIntervalPace(
                            (inserts - lastInserts) /
                            static_cast<double>(m_interval) *
                            megsPerHour);

                    std::cout <<
                        formatTime(s) << " - " <<
                        std::round(progress * 100.0) << "% - " <<
                        commify(inserts) << " - " <<
                        commify(totalPace) <<
                        "(" << commify(lastIntervalPace) << ")M/h - " <<
                        info.written << "W - " << info.read << "R - " <<
                        info.alive << "A" <<
                        std::endl;
                }

                lastInserts = inserts;
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
        });
    }

    if (verbose())
    {
        std::cout << "\tPushes complete - joining..." << std::endl;
    }

    save();
}

void Builder::insertPath(const Origin originId, FileInfo& info)
{
    const std::string rawPath(info.path());
    std::size_t tries(0);
    std::unique_ptr<arbiter::LocalHandle> localHandle;

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

    uint64_t inserted(0);
    uint64_t pointId(0);

    ChunkKey ck(*m_metadata);
    Clipper clipper(m_registry->cache());

    VectorPointTable table(m_metadata->schema());
    table.setProcess([&]()
    {
        inserted += table.numPoints();

        if (inserted > m_sleepCount)
        {
            inserted = 0;
            clipper.clip();
        }

        std::unique_ptr<ScaleOffset> so(m_metadata->outSchema().scaleOffset());

        Voxel voxel;

        PointStats pointStats;
        const Bounds& boundsConforming(m_metadata->boundsConforming());
        const Bounds* boundsSubset(m_metadata->boundsSubset());

        Key key(*m_metadata);

        for (auto it(table.begin()); it != table.end(); ++it)
        {
            auto& pr(it.pointRef());
            pr.setField(DimId::OriginId, originId);
            pr.setField(DimId::PointId, pointId);
            ++pointId;

            voxel.initShallow(it.pointRef(), it.data());
            if (so) voxel.clip(*so);
            const Point& point(voxel.point());

            ck.reset();

            if (boundsConforming.contains(point))
            {
                if (!boundsSubset || boundsSubset->contains(point))
                {
                    key.init(point);
                    m_registry->addPoint(voxel, key, ck, clipper);
                    pointStats.addInsert();
                }
            }
            else if (m_metadata->primary()) pointStats.addOutOfBounds();
        }

        if (originId != invalidOrigin)
        {
            m_metadata->mutableFiles().add(originId, pointStats);
        }
    });

    const json pipeline(m_config.pipeline(localPath));

    if (!Executor::get().run(table, pipeline))
    {
        throw std::runtime_error("Failed to execute: " + rawPath);
    }
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

    if (verbose()) std::cout << "Reawakened: " << reawakened << std::endl;

    if (verbose()) std::cout << "Saving registry..." << std::endl;
    m_registry->save(m_config.hierarchyStep(), verbose());

    if (verbose()) std::cout << "Saving metadata..." << std::endl;
    m_metadata->save(*m_out, m_config);
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

        if (!arbiter::mkdirp(m_tmp->root()))
        {
            throw std::runtime_error("Couldn't create tmp directory");
        }

        const std::string rootDir(m_out->root());
        if (!m_out->isRemote())
        {
            if (!arbiter::mkdirp(rootDir))
            {
                throw std::runtime_error("Couldn't create " + rootDir);
            }

            if (!arbiter::mkdirp(rootDir + "ept-data"))
            {
                throw std::runtime_error("Couldn't create data directory");
            }

            if (!arbiter::mkdirp(rootDir + "ept-hierarchy"))
            {
                throw std::runtime_error("Couldn't create hierarchy directory");
            }

            if (!arbiter::mkdirp(rootDir + "ept-sources"))
            {
                throw std::runtime_error("Couldn't create sources directory");
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

const arbiter::Endpoint& Builder::outEndpoint() const { return *m_out; }
const arbiter::Endpoint& Builder::tmpEndpoint() const { return *m_tmp; }

std::mutex& Builder::mutex() { return m_mutex; }

} // namespace entwine


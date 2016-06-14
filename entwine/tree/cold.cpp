/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/cold.hpp>

#include <chrono>
#include <thread>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/tree/thread-pools.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/storage.hpp>

namespace entwine
{

namespace
{
    const std::size_t maxCreateTries(8);
    const auto createSleepTime(std::chrono::milliseconds(500));

    const std::size_t maxFastTrackers(std::pow(4, 12));
}

Cold::Cold(const Builder& builder, bool exists)
    : m_builder(builder)
    , m_chunkVec(getNumFastTrackers(m_builder.metadata().structure()))
    , m_chunkMap()
    , m_mapMutex()
    , m_pool(m_builder.threadPools().clipPool())
{
    if (exists)
    {
        const Json::Value json(([this]()
        {
            const auto endpoint(m_builder.outEndpoint());
            const auto postfix(m_builder.metadata().postfix());
            const std::string subpath("entwine-ids" + postfix);

            return parse(m_builder.outEndpoint().get(subpath));
        })());

        Id id(0);

        const Structure& structure(m_builder.metadata().structure());

        for (Json::ArrayIndex i(0); i < json.size(); ++i)
        {
            id = Id(json[i].asString());

            const ChunkInfo chunkInfo(structure.getInfo(id));
            const std::size_t chunkNum(chunkInfo.chunkNum());

            if (chunkNum < m_chunkVec.size())
            {
                m_chunkVec[chunkNum].mark.store(true);
            }
            else
            {
                std::unique_ptr<CountedChunk> c(new CountedChunk());
                m_chunkMap.emplace(id, std::move(c));
            }
        }
    }
}

Cold::~Cold()
{ }

Tube::Insertion Cold::insert(
        const Climber& climber,
        Clipper& clipper,
        Cell::PooledNode& cell)
{
    CountedChunk* countedChunk(nullptr);

    const std::size_t chunkNum(climber.chunkNum());
    const Id& chunkId(climber.chunkId());

    if (chunkNum < m_chunkVec.size())
    {
        growFast(climber, clipper);
        countedChunk = m_chunkVec[chunkNum].chunk.get();
    }
    else
    {
        growSlow(climber, clipper);

        std::lock_guard<std::mutex> mapLock(m_mapMutex);
        countedChunk = m_chunkMap.at(chunkId).get();
    }

    if (!countedChunk)
    {
        throw std::runtime_error("CountedChunk has missing contents.");
    }

    return countedChunk->chunk->insert(climber, cell);
}

std::set<Id> Cold::ids() const
{
    std::set<Id> results(m_fauxIds);

    const Structure& structure(m_builder.metadata().structure());

    for (std::size_t i(0); i < m_chunkVec.size(); ++i)
    {
        if (m_chunkVec[i].mark.load())
        {
            ChunkInfo info(structure.getInfoFromNum(i));
            results.insert(info.chunkId());
        }
    }

    std::lock_guard<std::mutex> lock(m_mapMutex);
    for (const auto& p : m_chunkMap)
    {
        results.insert(p.first);
    }

    return results;
}

void Cold::save(const arbiter::Endpoint& endpoint) const
{
    m_pool.join();

    Json::Value json;
    for (const auto& id : ids())
    {
        json.append(id.str());
    }

    const std::string subpath("entwine-ids" + m_builder.metadata().postfix());
    endpoint.put(subpath, toFastString(json));
}

void Cold::growFast(const Climber& climber, Clipper& clipper)
{
    const Id& chunkId(climber.chunkId());
    const std::size_t chunkNum(climber.chunkNum());

    if (clipper.insert(chunkId, chunkNum))
    {
        FastSlot& slot(m_chunkVec[chunkNum]);

        UniqueSpin lock(slot.spinner);

        const bool exists(slot.mark.load());
        slot.mark.store(true);
        auto& countedChunk(slot.chunk);

        if (!countedChunk) countedChunk.reset(new CountedChunk());

        std::lock_guard<std::mutex> chunkLock(countedChunk->mutex);
        lock.unlock();

        if (!countedChunk->refs.count(clipper.id()))
        {
            countedChunk->refs[clipper.id()] = 1;
        }
        else
        {
            ++countedChunk->refs[clipper.id()];
        }

        ensureChunk(climber, countedChunk->chunk, exists);
    }
}

void Cold::growSlow(const Climber& climber, Clipper& clipper)
{
    const Id& chunkId(climber.chunkId());

    if (clipper.insert(chunkId, climber.chunkNum()))
    {
        std::unique_lock<std::mutex> mapLock(m_mapMutex);

        const bool exists(m_chunkMap.count(chunkId));
        auto& countedChunk(m_chunkMap[chunkId]);

        if (!exists) countedChunk.reset(new CountedChunk());

        std::lock_guard<std::mutex> chunkLock(countedChunk->mutex);
        mapLock.unlock();

        if (!countedChunk->refs.count(clipper.id()))
        {
            countedChunk->refs[clipper.id()] = 1;
        }
        else
        {
            ++countedChunk->refs[clipper.id()];
        }

        ensureChunk(climber, countedChunk->chunk, exists);
    }
}

void Cold::growFaux(const Id& id)
{
    m_fauxIds.insert(id);
}

void Cold::ensureChunk(
        const Climber& climber,
        std::unique_ptr<Chunk>& chunk,
        const bool exists)
{
    const Id& chunkId(climber.chunkId());

    std::size_t tries(0);
    while (!chunk)
    {
        if (exists)
        {
            const std::string path(
                    m_builder.metadata().structure().maybePrefix(chunkId) +
                    m_builder.metadata().postfix(true));

            auto data(Storage::ensureGet(m_builder.outEndpoint(), path));

            chunk =
                    Chunk::create(
                        m_builder,
                        climber.depth(),
                        chunkId,
                        climber.pointsPerChunk(),
                        std::move(data));
        }
        else
        {
            chunk =
                    Chunk::create(
                        m_builder,
                        climber.depth(),
                        chunkId,
                        climber.pointsPerChunk());
        }

        if (!chunk)
        {
            if (++tries < maxCreateTries)
            {
                std::cout <<
                    "Failed chunk create on " <<
                    (exists ? "existing" : "new") << " chunk: " << chunkId <<
                    std::endl;
                std::this_thread::sleep_for(createSleepTime);
            }
            else
            {
                std::cout << "Invalid chunk at " << chunkId << std::endl;
                std::cout << "Non-recoverable error - exiting" << std::endl;
                exit(1);
            }
        }
    }
}

void Cold::clip(
        const Id& chunkId,
        const std::size_t chunkNum,
        const std::size_t id)
{
    if (chunkNum < m_chunkVec.size())
    {
        FastSlot& slot(m_chunkVec[chunkNum]);
        CountedChunk& countedChunk(*slot.chunk);

        m_pool.add([this, &countedChunk, id]()
        {
            unrefChunk(countedChunk, id, true);
        });
    }
    else
    {
        std::unique_lock<std::mutex> mapLock(m_mapMutex);
        CountedChunk& countedChunk(*m_chunkMap.at(chunkId));
        mapLock.unlock();

        m_pool.add([this, &countedChunk, id]()
        {
            unrefChunk(countedChunk, id, false);
        });
    }
}

void Cold::unrefChunk(
        CountedChunk& countedChunk,
        const std::size_t id,
        const bool fast)
{
    std::lock_guard<std::mutex> chunkLock(countedChunk.mutex);

    if (!--countedChunk.refs.at(id)) countedChunk.refs.erase(id);

    if (countedChunk.refs.empty())
    {
        if (countedChunk.chunk)
        {
            countedChunk.chunk.reset(nullptr);
        }
        else
        {
            std::cout << "Tried to clip null chunk " << id << " - ";
            std::cout << (fast ? "fast" : "slow") << std::endl;
            exit(1);
        }
    }
}

void Cold::merge(const Cold& other)
{
    for (const Id& id : other.ids()) m_fauxIds.insert(id);
}

std::size_t Cold::getNumFastTrackers(const Structure& structure)
{
    std::size_t count(0);
    std::size_t depth(structure.coldDepthBegin());

    while (
            count < maxFastTrackers &&
            depth < 64 &&
            (depth < structure.coldDepthEnd() || !structure.coldDepthEnd()))
    {
        count += structure.numChunksAtDepth(depth);
        ++depth;
    }

    return count;
}

Cold::CountedChunk::~CountedChunk() { }

} // namespace entwine


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
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/storage.hpp>

namespace entwine
{

namespace
{
    const std::size_t maxCreateTries(8);
    const auto createSleepTime(std::chrono::milliseconds(500));

    const std::size_t maxFastTrackers(std::pow(4, 12));

    std::size_t getNumFastTrackers(const Structure& structure)
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
}

Cold::Cold(arbiter::Endpoint& endpoint, const Builder& builder)
    : m_endpoint(endpoint)
    , m_builder(builder)
    , m_chunkVec(getNumFastTrackers(builder.structure()))
    , m_chunkMap()
    , m_mapMutex()
{ }

Cold::Cold(
        arbiter::Endpoint& endpoint,
        const Builder& builder,
        const Json::Value& jsonIds)
    : m_endpoint(endpoint)
    , m_builder(builder)
    , m_chunkVec(getNumFastTrackers(builder.structure()))
    , m_chunkMap()
    , m_mapMutex()
{
    if (jsonIds.isArray())
    {
        Id id(0);

        const Structure& structure(m_builder.structure());

        for (std::size_t i(0); i < jsonIds.size(); ++i)
        {
            id = Id(jsonIds[static_cast<Json::ArrayIndex>(i)].asString());

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

Cell& Cold::getCell(const Climber& climber, Clipper* clipper)
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

    return countedChunk->chunk->getCell(climber);
}

std::set<Id> Cold::ids() const
{
    std::set<Id> results(m_fauxIds);

    const Structure& structure(m_builder.structure());

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

Json::Value Cold::toJson() const
{
    Json::Value json;
    for (const auto& id : ids()) json.append(id.str());
    return json;
}

void Cold::growFast(const Climber& climber, Clipper* clipper)
{
    const Id& chunkId(climber.chunkId());
    const std::size_t chunkNum(climber.chunkNum());

    if (clipper && clipper->insert(chunkId, chunkNum))
    {
        FastSlot& slot(m_chunkVec[chunkNum]);

        while (slot.flag.test_and_set())
            ;

        const bool exists(slot.mark.load());
        slot.mark.store(true);
        auto& countedChunk(slot.chunk);

        if (!countedChunk) countedChunk.reset(new CountedChunk());

        std::lock_guard<std::mutex> chunkLock(countedChunk->mutex);
        slot.flag.clear();

        countedChunk->refs.insert(clipper);

        ensureChunk(climber, countedChunk->chunk, exists);
    }
}

void Cold::growSlow(const Climber& climber, Clipper* clipper)
{
    const Id& chunkId(climber.chunkId());

    if (clipper && clipper->insert(chunkId, climber.chunkNum()))
    {
        std::unique_lock<std::mutex> mapLock(m_mapMutex);

        const bool exists(m_chunkMap.count(chunkId));
        auto& countedChunk(m_chunkMap[chunkId]);

        if (!exists) countedChunk.reset(new CountedChunk());

        std::lock_guard<std::mutex> chunkLock(countedChunk->mutex);
        mapLock.unlock();

        countedChunk->refs.insert(clipper);

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
            chunk =
                    Chunk::create(
                        m_builder,
                        climber.bboxChunk(),
                        climber.depth(),
                        chunkId,
                        climber.chunkPoints(),
                        Storage::ensureGet(
                            m_endpoint,
                            m_builder.structure().maybePrefix(chunkId)));
        }
        else
        {
            chunk =
                    Chunk::create(
                        m_builder,
                        climber.bboxChunk(),
                        climber.depth(),
                        chunkId,
                        climber.chunkPoints(),
                        chunkId < m_builder.structure().mappedIndexBegin());
        }

        if (!chunk)
        {
            if (++tries < maxCreateTries)
            {
                std::cout << "Failed chunk create " << chunkId << std::endl;
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
        Clipper* clipper,
        Pool& pool)
{
    if (chunkNum < m_chunkVec.size())
    {
        FastSlot& slot(m_chunkVec[chunkNum]);
        CountedChunk& countedChunk(*slot.chunk);

        pool.add([this, &countedChunk, clipper]()
        {
            unrefChunk(countedChunk, clipper, true);
        });
    }
    else
    {
        std::unique_lock<std::mutex> mapLock(m_mapMutex);
        CountedChunk& countedChunk(*m_chunkMap.at(chunkId));
        mapLock.unlock();

        pool.add([this, &countedChunk, clipper]()
        {
            unrefChunk(countedChunk, clipper, false);
        });
    }
}

void Cold::unrefChunk(CountedChunk& countedChunk, Clipper* clipper, bool fast)
{
    std::lock_guard<std::mutex> chunkLock(countedChunk.mutex);
    countedChunk.refs.erase(clipper);

    if (countedChunk.refs.empty())
    {
        if (countedChunk.chunk)
        {
            countedChunk.chunk->save(m_endpoint);
            countedChunk.chunk.reset(nullptr);
        }
        else
        {
            std::cout << "Tried to clip null chunk - ";
            std::cout << (fast ? "fast" : "slow") << std::endl;
            exit(1);
        }
    }
}

void Cold::merge(const Cold& other)
{
    for (const Id& id : other.ids()) m_fauxIds.insert(id);
}

} // namespace entwine


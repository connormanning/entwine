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
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/single-point-table.hpp>
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

Cold::Cold(
        arbiter::Endpoint& endpoint,
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pointPool)
    : m_endpoint(endpoint)
    , m_schema(schema)
    , m_structure(structure)
    , m_pointPool(pointPool)
    , m_chunkVec(getNumFastTrackers(m_structure))
    , m_chunkMap()
    , m_mapMutex()
{ }

Cold::Cold(
        arbiter::Endpoint& endpoint,
        const Schema& schema,
        const BBox& bbox,
        const Structure& structure,
        Pools& pointPool,
        const Json::Value& meta)
    : m_endpoint(endpoint)
    , m_schema(schema)
    , m_structure(structure)
    , m_pointPool(pointPool)
    , m_chunkVec(getNumFastTrackers(m_structure))
    , m_chunkMap()
    , m_mapMutex()
{
    const Json::Value& jsonIds(meta["ids"]);

    if (!jsonIds.isArray())
    {
        throw std::runtime_error("Invalid saved state.");
    }

    Id id(0);

    for (std::size_t i(0); i < jsonIds.size(); ++i)
    {
        id = Id(jsonIds[static_cast<Json::ArrayIndex>(i)].asString());

        const ChunkInfo chunkInfo(m_structure.getInfo(id));
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

Cold::~Cold()
{ }

Cell& Cold::getCell(const Climber& climber, Clipper* clipper)
{
    CountedChunk* countedChunk(0);

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

Json::Value Cold::toJson() const
{
    Json::Value json;
    std::set<Id> ids;

    for (std::size_t i(0); i < m_chunkVec.size(); ++i)
    {
        if (m_chunkVec[i].mark.load())
        {
            ChunkInfo info(m_structure.getInfoFromNum(i));
            ids.insert(info.chunkId());
        }
    }

    std::lock_guard<std::mutex> lock(m_mapMutex);
    for (const auto& p : m_chunkMap)
    {
        ids.insert(p.first);
    }

    for (const auto& id : ids)
    {
        json.append(id.str());
    }

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

        std::size_t tries(0);
        while (!countedChunk->chunk)
        {
            if (exists)
            {
                countedChunk->chunk =
                        Chunk::create(
                            m_schema,
                            climber.bboxChunk(),
                            m_structure,
                            m_pointPool,
                            climber.depth(),
                            chunkId,
                            climber.chunkPoints(),
                            Storage::ensureGet(
                                m_endpoint,
                                m_structure.maybePrefix(chunkId)));
            }
            else
            {
                countedChunk->chunk =
                        Chunk::create(
                            m_schema,
                            climber.bboxChunk(),
                            m_structure,
                            m_pointPool,
                            climber.depth(),
                            chunkId,
                            climber.chunkPoints(),
                            chunkId < m_structure.mappedIndexBegin());
            }

            if (!countedChunk->chunk)
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

        std::size_t tries(0);
        while (!countedChunk->chunk)
        {
            if (exists)
            {
                countedChunk->chunk =
                        Chunk::create(
                            m_schema,
                            climber.bboxChunk(),
                            m_structure,
                            m_pointPool,
                            climber.depth(),
                            chunkId,
                            climber.chunkPoints(),
                            Storage::ensureGet(
                                m_endpoint,
                                m_structure.maybePrefix(chunkId)));
            }
            else
            {
                countedChunk->chunk =
                        Chunk::create(
                            m_schema,
                            climber.bboxChunk(),
                            m_structure,
                            m_pointPool,
                            climber.depth(),
                            chunkId,
                            climber.chunkPoints(),
                            chunkId < m_structure.mappedIndexBegin());
            }

            if (!countedChunk->chunk)
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
}

void Cold::clip(
        const Id& chunkId,
        const std::size_t chunkNum,
        Clipper* clipper,
        Pool& pool)
{
    if (chunkNum < m_chunkVec.size())
    {
        pool.add([this, clipper, chunkNum, &chunkId]()
        {
            FastSlot& slot(m_chunkVec[chunkNum]);
            CountedChunk& countedChunk(*slot.chunk);

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
                    std::cout << "Tried to clip null chunk (fast)" << std::endl;
                    std::cout << chunkId << std::endl;
                    exit(1);
                }
            }
        });
    }
    else
    {
        std::unique_lock<std::mutex> mapLock(m_mapMutex);
        CountedChunk& countedChunk(*m_chunkMap.at(chunkId));
        mapLock.unlock();

        pool.add([this, clipper, &countedChunk, &chunkId]()
        {
            std::lock_guard<std::mutex> chunkLock(countedChunk.mutex);
            countedChunk.refs.erase(clipper);

            if (countedChunk.refs.empty())
            {
                if (countedChunk.chunk)
                {
                    countedChunk.chunk->save(m_endpoint);
                    countedChunk.chunk.reset(0);
                }
                else
                {
                    std::cout << "Tried to clip null chunk (slow)" << std::endl;
                    std::cout << chunkId << std::endl;
                    exit(1);
                }
            }
        });
    }
}

} // namespace entwine


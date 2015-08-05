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

#include <entwine/third/json/json.h>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/single-point-table.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

namespace
{
    const std::size_t maxFastTrackers(std::pow(4, 12));

    std::size_t getNumFastTrackers(const Structure& structure)
    {
        std::size_t count(0);
        std::size_t depth(structure.coldDepthBegin());

        while (count < maxFastTrackers && depth < structure.coldDepthEnd())
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
        const Structure& structure,
        const std::vector<char>& empty)
    : m_endpoint(endpoint)
    , m_schema(schema)
    , m_structure(structure)
    , m_chunkVec(getNumFastTrackers(m_structure))
    , m_chunkMap()
    , m_mapMutex()
    , m_empty(empty)
{ }

Cold::Cold(
        arbiter::Endpoint& endpoint,
        const Schema& schema,
        const Structure& structure,
        const std::vector<char>& empty,
        const Json::Value& meta)
    : m_endpoint(endpoint)
    , m_schema(schema)
    , m_structure(structure)
    , m_chunkVec(getNumFastTrackers(m_structure))
    , m_chunkMap()
    , m_mapMutex()
    , m_empty(empty)
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
            m_chunkMap.insert(
                    std::make_pair(id, std::unique_ptr<CountedChunk>()));
        }
    }
}

Cold::~Cold()
{ }

Entry* Cold::getEntry(const Id& index, Clipper* clipper)
{
    CountedChunk* countedChunk(0);

    const ChunkInfo info(m_structure.getInfo(index));

    const std::size_t chunkNum(info.chunkNum());
    const Id& chunkId(info.chunkId());

    if (chunkNum < m_chunkVec.size())
    {
        growFast(info, clipper);
        countedChunk = m_chunkVec[chunkNum].chunk.get();
    }
    else
    {
        growSlow(info, clipper);

        std::lock_guard<std::mutex> mapLock(m_mapMutex);
        countedChunk = m_chunkMap.at(chunkId).get();
    }

    if (!countedChunk)
    {
        throw std::runtime_error("CountedChunk has missing contents.");
    }

    return countedChunk->chunk->getEntry(index);
}

Json::Value Cold::toJson() const
{
    Json::Value json;

    for (std::size_t i(0); i < m_chunkVec.size(); ++i)
    {
        if (m_chunkVec[i].mark.load())
        {
            ChunkInfo info(m_structure.getInfoFromNum(i));
            json.append(info.chunkId().str());
        }
    }

    std::lock_guard<std::mutex> lock(m_mapMutex);
    for (const auto& p : m_chunkMap)
    {
        json.append(p.first.str());
    }

    return json;
}

void Cold::growFast(const ChunkInfo& info, Clipper* clipper)
{
    const Id& chunkId(info.chunkId());
    const std::size_t chunkNum(info.chunkNum());

    if (clipper && clipper->insert(chunkId))
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

        if (!countedChunk->chunk)
        {
            if (exists)
            {
                countedChunk->chunk.reset(
                        new Chunk(
                            m_schema,
                            chunkId,
                            info.chunkPoints(),
                            m_endpoint.getSubpathBinary(chunkId.str()),
                            m_empty));
            }
            else
            {
                countedChunk->chunk.reset(
                        new Chunk(
                            m_schema,
                            chunkId,
                            info.chunkPoints(),
                            chunkId < m_structure.sparseIndexBegin(),
                            m_empty));
            }
        }
    }
}

void Cold::growSlow(const ChunkInfo& info, Clipper* clipper)
{
    const Id& chunkId(info.chunkId());

    if (clipper && clipper->insert(chunkId))
    {
        std::unique_lock<std::mutex> mapLock(m_mapMutex);

        const bool exists(m_chunkMap.count(chunkId));
        auto& countedChunk(m_chunkMap[chunkId]);

        if (!exists) countedChunk.reset(new CountedChunk());

        std::lock_guard<std::mutex> chunkLock(countedChunk->mutex);
        mapLock.unlock();

        countedChunk->refs.insert(clipper);

        if (!countedChunk->chunk)
        {
            if (exists)
            {
                countedChunk->chunk.reset(
                        new Chunk(
                            m_schema,
                            chunkId,
                            info.chunkPoints(),
                            m_endpoint.getSubpathBinary(chunkId.str()),
                            m_empty));
            }
            else
            {
                countedChunk->chunk.reset(
                        new Chunk(
                            m_schema,
                            chunkId,
                            info.chunkPoints(),
                            chunkId < m_structure.sparseIndexBegin(),
                            m_empty));
            }
        }
    }
}

void Cold::clip(const Id& chunkId, Clipper* clipper, Pool& pool)
{
    const ChunkInfo info(m_structure.getInfo(chunkId));
    const std::size_t chunkNum(info.chunkNum());

    if (chunkNum < m_chunkVec.size())
    {
        pool.add([this, clipper, &chunkId, chunkNum]()
        {
            FastSlot& slot(m_chunkVec[chunkNum]);
            CountedChunk& countedChunk(*slot.chunk);

            std::unique_lock<std::mutex> chunkLock(countedChunk.mutex);
            countedChunk.refs.erase(clipper);

            if (countedChunk.refs.empty())
            {
                countedChunk.chunk->save(m_endpoint);
                countedChunk.chunk.reset(0);
            }
        });
    }
    else
    {
        std::unique_lock<std::mutex> mapLock(m_mapMutex);
        CountedChunk& countedChunk(*m_chunkMap.at(chunkId));
        mapLock.unlock();

        pool.add([this, clipper, &countedChunk]()
        {
            std::lock_guard<std::mutex> chunkLock(countedChunk.mutex);
            countedChunk.refs.erase(clipper);

            if (countedChunk.refs.empty())
            {
                countedChunk.chunk->save(m_endpoint);
                countedChunk.chunk.reset(0);
            }
        });
    }
}

} // namespace entwine


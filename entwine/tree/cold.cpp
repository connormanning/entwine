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

#include <entwine/drivers/source.hpp>
#include <entwine/third/json/json.h>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/single-point-table.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

namespace
{
    std::size_t getFastTrackers(const Structure& structure)
    {
        std::size_t coldDepths(
                structure.coldDepth() - structure.baseDepth());

        std::size_t fastTrackers(
                structure.coldFirstSpan() / structure.chunkPoints());

        for (std::size_t i(0); i < coldDepths; ++i)
        {
            if (fastTrackers > std::pow(4, 12))
            {
                break;
            }

            fastTrackers *= 4;
        }

        return fastTrackers;
    }
}

Cold::Cold(
        Source& source,
        const Schema& schema,
        const Structure& structure,
        const std::vector<char>& empty)
    : m_source(source)
    , m_schema(schema)
    , m_structure(structure)
    , m_chunkVec(getFastTrackers(m_structure))
    , m_chunkMap()
    , m_mapMutex()
    , m_slowStart()
    , m_empty(empty)
{
    m_slowStart =
            m_structure.coldIndexBegin() +
            m_chunkVec.size() * m_structure.chunkPoints();
}

Cold::Cold(
        Source& source,
        const Schema& schema,
        const Structure& structure,
        const std::vector<char>& empty,
        const Json::Value& meta)
    : m_source(source)
    , m_schema(schema)
    , m_structure(structure)
    , m_chunkVec(getFastTrackers(m_structure))
    , m_chunkMap()
    , m_mapMutex()
    , m_slowStart()
    , m_empty(empty)
{
    m_slowStart =
            m_structure.coldIndexBegin() +
            m_chunkVec.size() * m_structure.chunkPoints();

    const Json::Value& jsonIds(meta["ids"]);

    if (!jsonIds.isArray())
    {
        throw std::runtime_error("Invalid saved state.");
    }

    for (std::size_t i(0); i < jsonIds.size(); ++i)
    {
        const std::size_t id(
                jsonIds[static_cast<Json::ArrayIndex>(i)].asUInt64());

        if (id < m_slowStart)
        {
            m_chunkVec[getChunkNum(id)].mark.store(true);
        }
        else
        {
            m_chunkMap.insert(std::make_pair(id, std::unique_ptr<ChunkInfo>()));
        }
    }
}

Cold::~Cold()
{ }

Entry* Cold::getEntry(std::size_t index, Clipper* clipper)
{
    const std::size_t chunkId(getChunkId(index));

    if (chunkId < m_slowStart)
        growFast(chunkId, clipper);
    else
        growSlow(chunkId, clipper);

    ChunkInfo* chunkInfo(0);

    if (chunkId < m_slowStart)
    {
        chunkInfo = m_chunkVec[getChunkNum(index)].chunk.get();
    }
    else
    {
        std::lock_guard<std::mutex> mapLock(m_mapMutex);
        chunkInfo = m_chunkMap.at(chunkId).get();
    }

    assert(chunkInfo);

    return chunkInfo->chunk->getEntry(index);
}

Json::Value Cold::toJson() const
{
    Json::Value json;

    const std::size_t start(m_structure.coldIndexBegin());
    const std::size_t chunkPoints(m_structure.chunkPoints());

    for (std::size_t i(0); i < m_chunkVec.size(); ++i)
    {
        if (m_chunkVec[i].mark.load())
        {
            json.append(static_cast<Json::UInt64>(start + chunkPoints * i));
        }
    }

    std::lock_guard<std::mutex> lock(m_mapMutex);
    for (const auto& p : m_chunkMap)
    {
        json.append(static_cast<Json::UInt64>(p.first));
    }

    return json;
}

std::size_t Cold::getChunkId(const std::size_t index) const
{
    assert(index >= m_structure.coldIndexBegin());

    const std::size_t chunkPoints(m_structure.chunkPoints());
    const std::size_t indexBegin(m_structure.coldIndexBegin());

    return indexBegin + getChunkNum(index) * chunkPoints;
}

std::size_t Cold::getChunkNum(const std::size_t index) const
{
    const std::size_t indexBegin(m_structure.coldIndexBegin());

    assert(index >= indexBegin);

    return (index - indexBegin) / m_structure.chunkPoints();
}

void Cold::growFast(const std::size_t chunkId, Clipper* clipper)
{
    if (clipper && clipper->insert(chunkId))
    {
        FastSlot& slot(m_chunkVec[getChunkNum(chunkId)]);

        while (slot.flag.test_and_set()) ;

        const bool exists(slot.mark.load());
        slot.mark.store(true);
        auto& chunkInfo(slot.chunk);

        if (!chunkInfo) chunkInfo.reset(new ChunkInfo());

        std::lock_guard<std::mutex> chunkLock(chunkInfo->mutex);
        slot.flag.clear();

        chunkInfo->refs.insert(clipper);

        if (!chunkInfo->chunk)
        {
            if (exists)
            {
                chunkInfo->chunk.reset(
                        new Chunk(
                            m_schema,
                            chunkId,
                            m_structure.chunkPoints(),
                            m_source.get(std::to_string(chunkId)),
                            m_empty));
            }
            else
            {
                chunkInfo->chunk.reset(
                        new Chunk(
                            m_schema,
                            chunkId,
                            m_structure.chunkPoints(),
                            chunkId < m_structure.sparseIndexBegin(),
                            m_empty));
            }
        }
    }
}

void Cold::growSlow(const std::size_t chunkId, Clipper* clipper)
{
    if (clipper && clipper->insert(chunkId))
    {
        std::unique_lock<std::mutex> mapLock(m_mapMutex);

        const bool exists(m_chunkMap.count(chunkId));
        auto& chunkInfo(m_chunkMap[chunkId]);

        if (!exists) chunkInfo.reset(new ChunkInfo());

        std::lock_guard<std::mutex> chunkLock(chunkInfo->mutex);
        mapLock.unlock();

        chunkInfo->refs.insert(clipper);

        if (!chunkInfo->chunk)
        {
            if (exists)
            {
                chunkInfo->chunk.reset(
                        new Chunk(
                            m_schema,
                            chunkId,
                            m_structure.chunkPoints(),
                            m_source.get(std::to_string(chunkId)),
                            m_empty));
            }
            else
            {
                chunkInfo->chunk.reset(
                        new Chunk(
                            m_schema,
                            chunkId,
                            m_structure.chunkPoints(),
                            chunkId < m_structure.sparseIndexBegin(),
                            m_empty));
            }
        }
    }
}

void Cold::clip(const std::size_t chunkId, Clipper* clipper, Pool& pool)
{
    if (chunkId < m_slowStart)
    {
        pool.add([this, clipper, chunkId]()
        {
            FastSlot& slot(m_chunkVec[getChunkNum(chunkId)]);
            ChunkInfo& chunkInfo(*slot.chunk);

            std::lock_guard<std::mutex> chunkLock(slot.chunk->mutex);
            chunkInfo.refs.erase(clipper);

            if (chunkInfo.refs.empty())
            {
                chunkInfo.chunk->save(m_source);
                slot.chunk.reset(0);
            }
        });
    }
    else
    {
        std::unique_lock<std::mutex> mapLock(m_mapMutex);
        ChunkInfo& chunkInfo(*m_chunkMap.at(chunkId));
        mapLock.unlock();

        pool.add([this, clipper, &chunkInfo]()
        {
            std::lock_guard<std::mutex> chunkLock(chunkInfo.mutex);
            chunkInfo.refs.erase(clipper);

            if (chunkInfo.refs.empty())
            {
                chunkInfo.chunk->save(m_source);
                chunkInfo.chunk.reset(0);
            }
        });
    }
}

} // namespace entwine


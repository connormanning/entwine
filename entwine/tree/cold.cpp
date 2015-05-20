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
    std::set<std::size_t> loadIds(const Json::Value& meta)
    {
        std::set<std::size_t> ids;
        const Json::Value jsonIds(meta["ids"]);

        if (!jsonIds.isArray())
        {
            throw std::runtime_error("Invalid saved state.");
        }

        for (std::size_t i(0); i < jsonIds.size(); ++i)
        {
            ids.insert(jsonIds[static_cast<Json::ArrayIndex>(i)].asUInt64());
        }

        return ids;
    }
}

Cold::Cold(
        Source& source,
        const Schema& schema,
        const Structure& structure)
    : m_source(source)
    , m_schema(schema)
    , m_structure(structure)
    , m_mutex()
    , m_ids()
    , m_chunks()
{ }

Cold::Cold(
        Source& source,
        const Schema& schema,
        const Structure& structure,
        const Json::Value& meta)
    : m_source(source)
    , m_schema(schema)
    , m_structure(structure)
    , m_mutex()
    , m_ids(loadIds(meta))
    , m_chunks()
{
    const Json::Value& jsonIds(meta["ids"]);

    if (!jsonIds.isArray())
    {
        throw std::runtime_error("Invalid saved state.");
    }

    for (std::size_t i(0); i < jsonIds.size(); ++i)
    {
        m_ids.insert(jsonIds[static_cast<Json::ArrayIndex>(i)].asUInt64());
    }
}

Cold::~Cold()
{ }

Entry* Cold::getEntry(std::size_t index, Clipper* clipper)
{
    const std::size_t chunkId(getChunkId(index));

    grow(chunkId, clipper);

    std::unique_lock<std::mutex> mapLock(m_mutex);

    assert(m_ids.count(chunkId));
    ChunkInfo& chunkInfo(m_chunks.at(chunkId));

    return chunkInfo.chunk->getEntry(index);
}

Json::Value Cold::toJson() const
{
    Json::Value json;
    Json::ArrayIndex i(0);

    std::lock_guard<std::mutex> lock(m_mutex);

    json.resize(m_ids.size());
    assert(m_chunks.empty());

    for (const auto id : m_ids)
    {
        json[i++] = static_cast<Json::UInt64>(id);
    }

    return json;
}

std::size_t Cold::getChunkId(const std::size_t index) const
{
    assert(index >= m_structure.coldIndexBegin());

    const std::size_t chunkPoints(m_structure.chunkPoints());

    const std::size_t indexBegin(m_structure.coldIndexBegin());
    const std::size_t slotId((index - indexBegin) / chunkPoints);

    return indexBegin + slotId * chunkPoints;
}

void Cold::grow(const std::size_t chunkId, Clipper* clipper)
{
    // TODO Check locking here.
    if (clipper && clipper->insert(chunkId))
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        ChunkInfo& chunkInfo(m_chunks[chunkId]);

        std::lock_guard<std::mutex> chunkLock(chunkInfo.mutex);

        // We are holding the lock for this chunknow, so this condition will
        // hold through the rest of this function.
        const bool exists(m_ids.count(chunkId));

        lock.unlock();

        chunkInfo.refs.insert(clipper);

        if (!chunkInfo.chunk)
        {
            if (exists)
            {
                chunkInfo.chunk.reset(
                        new Chunk(
                            m_schema,
                            chunkId,
                            m_structure.chunkPoints(),
                            m_source.get(std::to_string(chunkId))));
            }
            else
            {
                lock.lock();
                m_ids.insert(chunkId);
                lock.unlock();

                chunkInfo.chunk.reset(
                        new Chunk(
                            m_schema,
                            chunkId,
                            m_structure.chunkPoints()));
            }
        }
    }
}

void Cold::clip(const std::size_t chunkId, Clipper* clipper)
{
    std::unique_lock<std::mutex> mapLock(m_mutex);
    ChunkInfo& chunkInfo(m_chunks.at(chunkId));
    mapLock.unlock();

    std::unique_lock<std::mutex> lock(chunkInfo.mutex);
    chunkInfo.refs.erase(clipper);

    if (chunkInfo.refs.empty())
    {
        chunkInfo.chunk->save(m_source);

        mapLock.lock();
        lock.unlock();

        m_chunks.erase(chunkId);
    }
}

} // namespace entwine


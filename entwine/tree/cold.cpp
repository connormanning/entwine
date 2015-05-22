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

Cold::Cold(
        Source& source,
        const Schema& schema,
        const Structure& structure,
        const std::vector<char>& empty)
    : m_source(source)
    , m_schema(schema)
    , m_structure(structure)
    , m_mutex()
    , m_chunks()
    , m_empty(empty)
{ }

Cold::Cold(
        Source& source,
        const Schema& schema,
        const Structure& structure,
        const std::vector<char>& empty,
        const Json::Value& meta)
    : m_source(source)
    , m_schema(schema)
    , m_structure(structure)
    , m_mutex()
    , m_chunks()
    , m_empty(empty)
{
    const Json::Value& jsonIds(meta["ids"]);

    if (!jsonIds.isArray())
    {
        throw std::runtime_error("Invalid saved state.");
    }

    for (std::size_t i(0); i < jsonIds.size(); ++i)
    {
        m_chunks.insert(
                std::make_pair(
                    jsonIds[static_cast<Json::ArrayIndex>(i)].asUInt64(),
                    std::unique_ptr<ChunkInfo>()));
    }
}

Cold::~Cold()
{ }

Entry* Cold::getEntry(std::size_t index, Clipper* clipper)
{
    const std::size_t chunkId(getChunkId(index));

    grow(chunkId, clipper);

    std::unique_lock<std::mutex> mapLock(m_mutex);
    ChunkInfo& chunkInfo(*m_chunks.at(chunkId));
    mapLock.unlock();

    return chunkInfo.chunk->getEntry(index);
}

Json::Value Cold::toJson() const
{
    Json::Value json;
    Json::ArrayIndex i(0);

    std::lock_guard<std::mutex> lock(m_mutex);

    json.resize(m_chunks.size());

    for (const auto& p : m_chunks)
    {
        json[i++] = static_cast<Json::UInt64>(p.first);
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
    if (clipper && clipper->insert(chunkId))
    {
        std::unique_lock<std::mutex> mapLock(m_mutex);

        const bool exists(m_chunks.count(chunkId));
        auto& chunkInfo(m_chunks[chunkId]);

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
    std::unique_lock<std::mutex> mapLock(m_mutex);
    ChunkInfo& chunkInfo(*m_chunks.at(chunkId));
    mapLock.unlock();

    pool.add([this, clipper, &chunkInfo]()
    {
        std::unique_lock<std::mutex> chunkLock(chunkInfo.mutex);
        chunkInfo.refs.erase(clipper);

        if (chunkInfo.refs.empty())
        {
            chunkInfo.chunk->save(m_source);
            chunkInfo.chunk.reset(0);
        }
    });
}

} // namespace entwine


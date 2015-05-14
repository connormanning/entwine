/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/branches/cold.hpp>

#include <entwine/compression/util.hpp>
#include <entwine/tree/branch.hpp>
#include <entwine/tree/branches/chunk.hpp>
#include <entwine/tree/branches/clipper.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/single-point-table.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

ColdBranch::ColdBranch(
        Source& source,
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t chunkPoints,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
    : Branch(source, schema, dimensions, depthBegin, depthEnd)
    , m_chunkPoints(chunkPoints)
    , m_mutex()
    , m_chunks()
{
    if (indexSpan() % chunkPoints)
    {
        throw std::runtime_error("Invalid chunk size.");
    }
}

ColdBranch::ColdBranch(
        Source& source,
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t chunkPoints,
        const Json::Value& meta)
    : Branch(source, schema, dimensions, meta)
    , m_chunkPoints(chunkPoints)
    , m_mutex()
    , m_chunks()
{ }

ColdBranch::~ColdBranch()
{ }

std::size_t ColdBranch::getChunkId(const std::size_t index) const
{
    assert(index >= indexBegin());
    return indexBegin() + getSlotId(index) * m_chunkPoints;
}

std::size_t ColdBranch::getSlotId(const std::size_t index) const
{
    return (index - indexBegin()) / m_chunkPoints;
}

std::unique_ptr<std::vector<char>> ColdBranch::fetch(
        const std::size_t chunkId) const
{
    std::vector<char> cmp(m_source.get(std::to_string(chunkId)));
    const std::size_t uncSize(Compression::popSize(cmp));

    return Compression::decompress(cmp, schema(), uncSize);
}

Entry& ColdBranch::getEntry(std::size_t index)
{
    const std::size_t chunkId(getChunkId(index));

    std::unique_lock<std::mutex> mapLock(m_mutex);

    assert(m_ids.count(chunkId));
    ChunkInfo& chunkInfo(m_chunks.at(chunkId));

    return chunkInfo.chunk->getEntry(index);
}

void ColdBranch::finalizeImpl(
        Source& output,
        Pool& pool,
        std::vector<std::size_t>& ids,
        const std::size_t start,
        const std::size_t exportChunkPoints)
{
    if (start > indexBegin())
    {
        throw std::runtime_error(
                "Cold start depth must be >= finalize base depth");
    }

    assert(m_chunks.empty());

    for (const std::size_t chunkId : m_ids)
    {
        pool.add([this, chunkId, &output, &ids, start, exportChunkPoints]()
        {
            Chunk chunk(
                    schema(),
                    chunkId,
                    m_chunkPoints,
                    m_source.get(std::to_string(chunkId)));

            chunk.finalize(output, ids, m_mutex, start, exportChunkPoints);
        });
    }
}

void ColdBranch::grow(Clipper* clipper, const std::size_t index)
{
    const std::size_t chunkId(getChunkId(index));

    if (clipper && clipper->insert(chunkId))
    {
        std::unique_lock<std::mutex> mapLock(m_mutex);
        ChunkInfo& chunkInfo(m_chunks[chunkId]);
        const bool exists(m_ids.count(chunkId));
        mapLock.unlock();

        std::lock_guard<std::mutex> lock(chunkInfo.mutex);

        chunkInfo.refs.insert(clipper);

        if (!chunkInfo.chunk)
        {
            if (exists)
            {
                chunkInfo.chunk.reset(
                        new Chunk(
                            schema(),
                            chunkId,
                            m_chunkPoints,
                            m_source.get(std::to_string(chunkId))));
            }
            else
            {
                m_ids.insert(chunkId);

                chunkInfo.chunk.reset(
                        new Chunk(schema(), chunkId, m_chunkPoints));
            }
        }
    }
}

void ColdBranch::clip(Clipper* clipper, const std::size_t chunkId)
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


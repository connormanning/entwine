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

std::unique_ptr<Entry> ColdBranch::getEntry(std::size_t index)
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
        pool.add([this, chunkId, &output, &ids, exportChunkPoints]()
        {
            std::vector<char> compressed(
                    m_source.get(std::to_string(chunkId)));

            const std::size_t pointSize(schema().pointSize());
            const std::size_t chunkBytes(exportChunkPoints * pointSize);

            if (exportChunkPoints < m_chunkPoints)
            {
                std::unique_ptr<std::vector<char>> uncompressed(
                        Compression::decompress(
                            compressed,
                            schema(),
                            m_chunkPoints * schema().pointSize()));

                for (
                        std::size_t offset(0);
                        offset < m_chunkPoints;
                        offset += exportChunkPoints)
                {
                    const std::size_t curId(chunkId + offset);
                    const char* pos(
                            uncompressed->data() + offset * pointSize);

                    std::unique_ptr<std::vector<char>> compressed(
                            Compression::compress(
                                pos,
                                chunkBytes,
                                schema()));

                    output.put(std::to_string(curId), *compressed);

                    std::lock_guard<std::mutex> lock(m_mutex);
                    ids.push_back(curId);
                }
            }
            else
            {
                output.put(std::to_string(chunkId), compressed);
                ids.push_back(chunkId);
            }
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
                std::vector<char> compressed(
                        m_source.get(std::to_string(chunkId)));

                std::unique_ptr<std::vector<char>> uncompressed(
                        Compression::decompress(
                            compressed,
                            schema(),
                            m_chunkPoints * schema().pointSize()));

                chunkInfo.chunk.reset(
                        new Chunk(schema(), chunkId, *uncompressed));
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

    std::lock_guard<std::mutex> lock(chunkInfo.mutex);
    chunkInfo.refs.erase(clipper);

    if (chunkInfo.refs.empty())
    {
        auto compressed(
                Compression::compress(
                    chunkInfo.chunk->data(),
                    schema()));

        m_source.put(std::to_string(chunkId), *compressed);

        mapLock.lock();
        m_chunks.erase(chunkId);
    }
}

} // namespace entwine



/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/reader/cache.hpp>

#include <entwine/tree/chunk.hpp>

namespace entwine
{

Block::Block(Cache& cache, ChunkMap chunks)
    : m_cache(cache)
    , m_chunks(chunks)
{ }

Block::~Block()
{
    m_cache.release(m_chunks);
}








Cache::Cache(const std::size_t maxChunks, const std::size_t maxChunksPerQuery)
    : m_maxChunks(std::max<std::size_t>(maxChunks, 16))
    , m_maxChunksPerQuery(std::max<std::size_t>(maxChunksPerQuery, 4))
    , m_chunks()
    , m_refs()
{ }

std::unique_ptr<Block> Cache::reserve(const ChunkIds& chunks)
{
    return std::unique_ptr<Block>();
}

void Cache::release(const ChunkMap& chunks)
{

}

} // namespace entwine


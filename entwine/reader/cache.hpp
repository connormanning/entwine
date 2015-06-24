/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <set>

#include <entwine/reader/reader.hpp>

namespace entwine
{

class Cache;
class ChunkReader;

typedef std::set<std::size_t> ChunkIds;

// RAII class to ensure the release (and subsequent de-ref-counting) of these
// reserved chunks.
class Block
{
    friend class Cache;

public:
    ~Block();
    const ChunkMap& chunkMap() { return m_chunks; }

private:
    Block(Cache& cache, ChunkMap chunks);

    Cache& m_cache;
    ChunkMap m_chunks;
};



class Cache
{
public:
    // maxChunks must be at least 16.
    // maxChunksPerQuery must be at least 4.
    Cache(std::size_t maxChunks, std::size_t maxChunksPerQuery);

    std::unique_ptr<Block> reserve(const ChunkIds& chunks);
    void release(const ChunkMap& chunks);

private:
    std::size_t m_maxChunks;
    std::size_t m_maxChunksPerQuery;

    std::map<std::size_t, std::unique_ptr<ChunkReader>> m_chunks;
    std::map<std::size_t, std::atomic_size_t> m_refs;
};

} // namespace entwine


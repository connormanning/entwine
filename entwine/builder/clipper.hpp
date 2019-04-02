/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <array>
#include <limits>

#include <entwine/types/key.hpp>

namespace entwine
{

class Chunk;
class ChunkCache;

struct CachedChunk
{
    CachedChunk()
        : xyz(
                std::numeric_limits<uint64_t>::max(),
                std::numeric_limits<uint64_t>::max(),
                std::numeric_limits<uint64_t>::max())
    { }

    CachedChunk(const Xyz& xyz) : xyz(xyz) { }

    Xyz xyz;
    Chunk* chunk = nullptr;
};

inline bool operator<(const CachedChunk& a, const CachedChunk& b)
{
    return a.xyz < b.xyz;
}

class Clipper
{
public:
    Clipper(ChunkCache& cache)
        : m_cache(cache)
    {
        m_fast.fill(CachedChunk());
    }

    ~Clipper();

    Chunk* get(const ChunkKey& ck);
    void set(const ChunkKey& ck, Chunk* chunk);
    void clip();

private:
    ChunkCache& m_cache;

    using UsedMap = std::map<Xyz, Chunk*>;
    using AgedSet = std::set<Xyz>;

    std::array<CachedChunk, maxDepth> m_fast;
    std::array<std::map<Xyz, Chunk*>, maxDepth> m_slow;
    std::array<std::map<Xyz, Chunk*>, maxDepth> m_aged;
};

} // namespace entwine


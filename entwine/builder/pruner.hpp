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

class NewChunk;
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
    NewChunk* chunk = nullptr;
};

inline bool operator<(const CachedChunk& a, const CachedChunk& b)
{
    return a.xyz < b.xyz;
}

class Pruner
{
public:
    Pruner(ChunkCache& cache)
        : m_cache(cache)
    {
        m_fast.fill(CachedChunk());
    }

    ~Pruner() { prune(); }

    NewChunk* get(const ChunkKey& ck);
    void set(const ChunkKey& ck, NewChunk* chunk);
    void prune();

private:
    ChunkCache& m_cache;

    std::array<CachedChunk, maxDepth> m_fast;
    std::array<std::map<Xyz, NewChunk*>, maxDepth> m_slow;
};

} // namespace entwine


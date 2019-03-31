/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/builder/pruner.hpp>

#include <entwine/builder/new-chunk.hpp>
#include <entwine/builder/chunk-cache.hpp>

namespace entwine
{

NewChunk* Pruner::get(const ChunkKey& ck)
{
    CachedChunk& fast(m_fast[ck.depth()]);

    if (fast.xyz == ck.position()) return fast.chunk;

    auto& slow(m_slow[ck.depth()]);
    auto it = slow.find(ck.position());

    if (it == slow.end()) return nullptr;

    fast.xyz = ck.position();
    return fast.chunk = it->second;
}

void Pruner::set(const ChunkKey& ck, NewChunk* chunk)
{
    CachedChunk& fast(m_fast[ck.depth()]);

    fast.xyz = ck.position();
    fast.chunk = chunk;

    auto& slow(m_slow[ck.depth()]);
    assert(!slow.count(ck.position()));
    slow[ck.position()] = chunk;
}

void Pruner::prune()
{
    std::cout << "Pruning" << std::endl;
    m_fast.fill(CachedChunk());

    for (
            uint64_t depth(0);
            depth < m_slow.size() && !m_slow[depth].empty();
            ++depth)
    {
        std::cout << "\tP " << depth << std::endl;
        auto& slice(m_slow[depth]);
        // TODO For now, we're just killing the entire slice.
        m_cache.prune(depth);
        slice.clear();
    }
}

} // namespace entwine


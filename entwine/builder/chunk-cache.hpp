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

#include <entwine/builder/chunk.hpp>
#include <entwine/builder/new-chunk.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/util/spin-lock.hpp>

namespace entwine
{

class Hierarchy;
class Pruner;

class ChunkCache
{
public:
    ChunkCache(
            Hierarchy& hierarchy,
            Pool& ioPool,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp);

    ~ChunkCache();

    void insert(Voxel& voxel, Key& key, const ChunkKey& ck, Pruner& pruner);
    void prune(uint64_t depth, const std::map<Xyz, NewChunk*>& stale);
    void purge();

private:
    NewChunk& addRef(const ChunkKey& ck, Pruner& pruner);

    Hierarchy& m_hierarchy;
    Pool& m_pool;
    const arbiter::Endpoint& m_out;
    const arbiter::Endpoint& m_tmp;

    std::array<SpinLock, maxDepth> m_spins;
    std::array<std::map<Xyz, std::unique_ptr<NewChunk>>, maxDepth> m_chunks;
};

} // namespace entwine


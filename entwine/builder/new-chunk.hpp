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

#include <cassert>
#include <cstddef>
#include <utility>

#include <entwine/builder/overflow.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/spin-lock.hpp>

namespace entwine
{

namespace arbiter { class Endpoint; }

class Hierarchy;
class ChunkCache;
class Pruner;

struct NewVoxelTube
{
    SpinLock spin;
    std::map<uint64_t, Voxel> map;
};

class NewChunk
{
public:
    NewChunk(const ChunkKey& ck, const Hierarchy& hierarchy);

    bool insert(ChunkCache& cache, Pruner& pruner, Voxel& voxel, Key& key);
    uint64_t save(const arbiter::Endpoint& out, const arbiter::Endpoint& tmp);
    void load(
            ChunkCache& cache,
            Pruner& pruner,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            uint64_t np);

    const ChunkKey& chunkKey() const { return m_chunkKey; }
    const ChunkKey& childAt(Dir dir) const
    {
        return m_childKeys[toIntegral(dir)];
    }

    SpinLock& spin() { return m_spin; }
    void addRef() { ++m_refs; }
    uint64_t delRef() { return --m_refs; }
    uint64_t refs() const { return m_refs; }

private:
    bool insertOverflow(
            ChunkCache& cache,
            Pruner& pruner,
            Voxel& voxel,
            Key& key);

    void maybeOverflow(ChunkCache& cache, Pruner& pruner);
    void doOverflow(ChunkCache& cache, Pruner& pruner, uint64_t dir);

    const Metadata& m_metadata;
    const uint64_t m_span;
    const uint64_t m_pointSize;
    const ChunkKey m_chunkKey;
    const std::array<ChunkKey, 8> m_childKeys;

    SpinLock m_spin;
    uint64_t m_refs = 1;

    std::vector<NewVoxelTube> m_grid;
    MemBlock m_gridBlock;

    SpinLock m_overflowSpin;
    std::array<std::unique_ptr<Overflow>, 8> m_overflows;
    uint64_t m_overflowCount = 0;
};

} // namespace entwine


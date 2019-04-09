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
class Clipper;

class VoxelTube
{
public:
    SpinLock& spin() { return m_spin; }
    Voxel& operator[](uint32_t i)
    {
        return m_map[i];
    }

private:
    SpinLock m_spin;
    std::map<uint32_t, Voxel> m_map;
};

class Chunk
{
public:
    Chunk(const ChunkKey& ck, const Hierarchy& hierarchy);

    bool insert(ChunkCache& cache, Clipper& clipper, Voxel& voxel, Key& key);
    uint64_t save(const arbiter::Endpoint& out, const arbiter::Endpoint& tmp)
        const;
    void load(
            ChunkCache& cache,
            Clipper& clipper,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            uint64_t np);

    const ChunkKey& chunkKey() const { return m_chunkKey; }
    const ChunkKey& childAt(Dir dir) const
    {
        return m_childKeys[toIntegral(dir)];
    }

    SpinLock& spin() { return m_spin; }

private:
    bool insertOverflow(
            ChunkCache& cache,
            Clipper& clipper,
            Voxel& voxel,
            Key& key);

    void maybeOverflow(ChunkCache& cache, Clipper& clipper);
    void doOverflow(ChunkCache& cache, Clipper& clipper, uint64_t dir);

    const Metadata& m_metadata;
    const uint64_t m_span;
    const uint64_t m_pointSize;
    const ChunkKey m_chunkKey;
    const std::array<ChunkKey, 8> m_childKeys;

    SpinLock m_spin;
    std::vector<VoxelTube> m_grid;
    MemBlock m_gridBlock;

    SpinLock m_overflowSpin;
    std::array<std::unique_ptr<Overflow>, 8> m_overflows;
    uint64_t m_overflowCount = 0;
};

} // namespace entwine


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

#include <entwine/builder/hierarchy.hpp>
#include <entwine/builder/overflow.hpp>
#include <entwine/io/io.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/endpoints.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/spin-lock.hpp>

namespace entwine
{

class ChunkCache;
class Clipper;

struct VoxelTube
{
    SpinLock spin;
    std::map<uint32_t, Voxel> map;
};

class Chunk
{
public:
    Chunk(
        const Metadata& m, 
        const Io& io,
        const ChunkKey& ck, 
        const Hierarchy& hierarchy);

    bool insert(ChunkCache& cache, Clipper& clipper, Voxel& voxel, Key& key);
    uint64_t save(const Endpoints& endpoints) const;
    void load(
        ChunkCache& cache,
        Clipper& clipper,
        const Endpoints& endpoints,
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
    const Io& m_io;
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


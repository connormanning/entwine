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
#include <entwine/builder/hierarchy.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/spin-lock.hpp>

namespace entwine
{

class Clipper;

class ReffedChunk
{
public:
    ReffedChunk(const Metadata& m, const ChunkKey& ck, const Hierarchy& h)
        : m_chunk(makeUnique<Chunk>(m, ck, h))
    { }

    SpinLock& spin() { return m_spin; }

    void add() { ++m_refs; }
    uint64_t del()
    {
        if (!m_refs) throw std::runtime_error("Negative");
        return --m_refs;
    }
    uint64_t count() const { return m_refs; }

    Chunk& chunk()
    {
        if (!m_chunk) throw std::runtime_error("Missing chunk");
        return *m_chunk;
    }

    void reset() { m_chunk.reset(); }
    bool exists() { return !!m_chunk; }
    void assign(const Metadata& m, const ChunkKey& ck, const Hierarchy& h)
    {
        assert(!exists());
        m_chunk = makeUnique<Chunk>(m, ck, h);
    }

private:
    SpinLock m_spin;
    uint64_t m_refs = 0;
    std::unique_ptr<Chunk> m_chunk;
};

class ChunkCache
{
public:
    ChunkCache(
        const Endpoints& endpoints,
        const Metadata& Metadata,
        Hierarchy& hierarchy,
        uint64_t threads);

    ~ChunkCache();

    void insert(Voxel& voxel, Key& key, const ChunkKey& ck, Clipper& clipper);
    void clip(uint64_t depth, const std::map<Xyz, Chunk*>& stale);
    void clipped() { maybePurge(m_cacheSize); }
    void join();

    struct Info
    {
        uint64_t written = 0;
        uint64_t read = 0;
        uint64_t alive = 0;
    };

    static Info latchInfo();

private:
    Chunk& addRef(const ChunkKey& ck, Clipper& clipper);
    void maybeSerialize(const Dxyz& dxyz);
    void maybeErase(const Dxyz& dxyz);
    void maybePurge(uint64_t maxCacheSize);

    const Endpoints& m_endpoints;
    const Metadata& m_metadata;
    Hierarchy& m_hierarchy;
    Pool m_pool;
    const uint64_t m_cacheSize = 64;

    std::array<SpinLock, maxDepth> m_spins;
    std::array<std::map<Xyz, ReffedChunk>, maxDepth> m_slices;

    SpinLock m_ownedSpin;
    std::set<Dxyz> m_owned;
};

} // namespace entwine


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

#include <entwine/builder/new-chunk.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/util/spin-lock.hpp>

namespace entwine
{

class Hierarchy;
class Pruner;

class NewReffedChunk
{
public:
    NewReffedChunk(const ChunkKey& ck, const Hierarchy& hierarchy)
        : m_chunk(makeUnique<NewChunk>(ck, hierarchy))
    { }

    SpinLock& spin() { return m_spin; }

    void add() { ++m_refs; }
    uint64_t del()
    {
        if (!m_refs) throw std::runtime_error("Negative");
        return --m_refs;
    }
    uint64_t count() const { return m_refs; }

    NewChunk& chunk()
    {
        if (!m_chunk) throw std::runtime_error("Missing chunk");
        return *m_chunk;
    }

    void reset() { m_chunk.reset(); }
    bool exists() { return !!m_chunk; }
    void assign(const ChunkKey& ck, const Hierarchy& hierarchy)
    {
        assert(!exists());
        m_chunk = makeUnique<NewChunk>(ck, hierarchy);
    }

private:
    SpinLock m_spin;
    uint64_t m_refs = 0;
    std::unique_ptr<NewChunk> m_chunk;
};

class ChunkCache
{
public:
    ChunkCache(
            Hierarchy& hierarchy,
            Pool& ioPool,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            uint64_t cacheSize);

    ~ChunkCache();

    void insert(Voxel& voxel, Key& key, const ChunkKey& ck, Pruner& pruner);
    void prune(uint64_t depth, const std::map<Xyz, NewChunk*>& stale);
    void pruned() { maybePurge(m_cacheSize); }

    struct Info
    {
        uint64_t written = 0;
        uint64_t read = 0;
        uint64_t alive = 0;
    };

    static Info latchInfo();

private:
    NewChunk& addRef(const ChunkKey& ck, Pruner& pruner);
    void maybeSerialize(const Dxyz& dxyz);
    void maybeErase(const Dxyz& dxyz);
    void maybePurge(uint64_t maxCacheSize);

    Hierarchy& m_hierarchy;
    Pool& m_pool;
    const arbiter::Endpoint& m_out;
    const arbiter::Endpoint& m_tmp;
    const uint64_t m_cacheSize = 64;

    std::array<SpinLock, maxDepth> m_spins;
    std::array<std::map<Xyz, NewReffedChunk>, maxDepth> m_slices;

    SpinLock m_ownedSpin;
    std::set<Dxyz> m_owned;
};

} // namespace entwine


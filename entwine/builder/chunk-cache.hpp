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
        assert(!m_refs);
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
            const arbiter::Endpoint& tmp);

    ~ChunkCache();

    void insert(Voxel& voxel, Key& key, const ChunkKey& ck, Pruner& pruner);
    void prune(uint64_t depth, const std::map<Xyz, NewChunk*>& stale);

    struct Info
    {
        uint64_t written = 0;
        uint64_t read = 0;
        uint64_t alive = 0;
    };

    Info latchInfo()
    {
        SpinGuard lock(m_infoSpin);
        Info latched = m_info;
        m_info.written = 0;
        m_info.read = 0;
        return latched;
    }

private:
    NewChunk& addRef(const ChunkKey& ck, Pruner& pruner);
    void maybeSerialize(const Dxyz& dxyz);
    void maybeErase(const Dxyz& dxyz);

    Hierarchy& m_hierarchy;
    Pool& m_pool;
    const arbiter::Endpoint& m_out;
    const arbiter::Endpoint& m_tmp;

    std::array<SpinLock, maxDepth> m_spins;
    std::array<std::map<Xyz, NewReffedChunk>, maxDepth> m_reffedChunks;

    SpinLock m_infoSpin;
    Info m_info;
};

} // namespace entwine


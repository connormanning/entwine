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

#include <limits>

#include <entwine/builder/chunk.hpp>
#include <entwine/builder/hierarchy.hpp>
#include <entwine/builder/new-chunk.hpp>
#include <entwine/util/spin-lock.hpp>

namespace entwine
{

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
    Pruner()
    {
        m_fast.fill(CachedChunk());
    }

    NewChunk* get(const ChunkKey& ck)
    {
        CachedChunk& fast(m_fast[ck.depth()]);

        if (fast.xyz == ck.position()) return fast.chunk;

        auto& slow(m_slow[ck.depth()]);
        auto it = slow.find(ck.position());

        if (it == slow.end()) return nullptr;

        fast.xyz = ck.position();
        return fast.chunk = it->second;
    }

    void set(const ChunkKey& ck, NewChunk* chunk)
    {
        CachedChunk& fast(m_fast[ck.depth()]);

        fast.xyz = ck.position();
        fast.chunk = chunk;

        auto& slow(m_slow[ck.depth()]);
        assert(!slow.count(ck.position()));
        slow[ck.position()] = chunk;
    }

    void clear()
    {
        m_fast.fill(CachedChunk());

        /*
        for (auto& slice : m_slow)
        {
            for (const auto& p : slice)
            {
                // TODO Unref these.
            }
        }
        */
    }

private:
    std::array<CachedChunk, 64> m_fast;
    std::array<std::map<Xyz, NewChunk*>, 64> m_slow;
};

/*
class Holder
{
public:
    Holder(const ChunkKey& ck)
        : m_chunkKey(ck)
        , m_chunk(makeUnique<NewChunk>(m_chunkKey.metadata()))
        , m_children { {
            ck.getStep(toDir(0)),
            ck.getStep(toDir(1)),
            ck.getStep(toDir(2)),
            ck.getStep(toDir(3)),
            ck.getStep(toDir(4)),
            ck.getStep(toDir(5)),
            ck.getStep(toDir(6)),
            ck.getStep(toDir(7))
        } }
    { }

    SpinLock& spin() { return m_spin; }

    void addRef() { ++m_refs; }
    uint64_t delRef() { return --m_refs; }

    void init(uint64_t np)
    {
        // TODO Fetch remote data.
    }

    void save(
            Hierarchy& hierarchy,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp)
    {
        const uint64_t np(m_chunk->save(out, tmp, m_chunkKey));
        hierarchy.set(m_chunkKey.get(), np);
    }

    NewChunk& chunk()
    {
        return *m_chunk;
    }

    const ChunkKey& childAt(uint64_t dir) const
    {
        return m_children[dir];
    }

    const ChunkKey& chunkKey() const { return m_chunkKey; }

private:
    SpinLock m_spin;
    uint64_t m_refs = 0;
    const ChunkKey m_chunkKey;
    std::unique_ptr<NewChunk> m_chunk;

    std::array<ChunkKey, 8> m_children;
};
*/

class ChunkCache
{
public:
    ChunkCache(
            Hierarchy& hierarchy,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp);

    ~ChunkCache();

    void insert(Voxel& voxel, Key& key, const ChunkKey& ck, Pruner& pruner,
            bool force = false);

private:
    NewChunk& addRef(const ChunkKey& ck);

    Hierarchy& m_hierarchy;
    const arbiter::Endpoint& m_out;
    const arbiter::Endpoint& m_tmp;

    SpinLock m_spin;
    std::map<Dxyz, NewChunk> m_chunks;
};

} // namespace entwine


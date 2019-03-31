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

class Holder;

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
    Holder* holder = nullptr;
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

    Holder* get(const ChunkKey& ck)
    {
        CachedChunk& fast(m_fast[ck.depth()]);

        if (fast.xyz == ck.position()) return fast.holder;

        auto& slow(m_slow[ck.depth()]);
        auto it = slow.find(ck.position());

        if (it == slow.end()) return nullptr;

        fast.xyz = ck.position();
        return fast.holder = it->second;
    }

    void set(const ChunkKey& ck, Holder* holder)
    {
        CachedChunk& fast(m_fast[ck.depth()]);

        fast.xyz = ck.position();
        fast.holder = holder;

        auto& slow(m_slow[ck.depth()]);
        assert(!slow.count(ck.position()));
        slow[ck.position()] = holder;
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
    std::array<std::map<Xyz, Holder*>, 64> m_slow;
};

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

class ChunkCache
{
public:
    ChunkCache(
            Hierarchy& hierarchy,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp)
        : m_hierarchy(hierarchy)
        , m_out(out)
        , m_tmp(tmp)
    { }

    ~ChunkCache()
    {
        for (auto& c : m_chunks)
        {
            c.second.save(m_hierarchy, m_out, m_tmp);
        }
    }

    void insert(Voxel& voxel, Key& key, const ChunkKey& ck, Pruner& pruner)
    {
        // Get from single-threaded cache if we can.
        Holder* holder = pruner.get(ck);

        // Otherwise, make sure it's initialized and increment its ref count.
        if (!holder)
        {
            holder = &addRef(ck);
            pruner.set(ck, holder);
        }

        // Try to insert the point into this chunk.
        if (holder->chunk().insert(voxel, key)) return;

        // TODO
        if (ck.depth() > 3) return;

        // Failed to insert - need to traverse to the next depth.
        key.step(voxel.point());
        const Dir dir(getDirection(ck.bounds().mid(), voxel.point()));
        insert(voxel, key, holder->childAt(toIntegral(dir)), pruner);
    }

private:
    // TODO This should be removed, instead using the single-threaded Pruner
    // for repeated access to a chunk.
    Holder& at(const ChunkKey& ck)
    {
        SpinGuard lock(m_spin);
        return m_chunks.at(ck.dxyz());
    }

    Holder& addRef(const ChunkKey& ck)
    {
        // This is the first access of this chunk for a particular thread.
        std::cout << "Creating " << ck.dxyz() << std::endl;

        // TODO This lock is egregious - narrow its scope to the relevant tube,
        // we need to split our chunk data structure into slices.
        UniqueSpin cacheLock(m_spin);

        auto it(m_chunks.find(ck.dxyz()));
        if (it != m_chunks.end())
        {
            // If we have the chunk, simply increment its ref count.
            Holder& holder = it->second;
            SpinGuard chunkLock(holder.spin());
            holder.addRef();
            return holder;
        }

        // Otherwise, create the chunk and initialize its contents.
        Holder& holder(
                m_chunks.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(ck.dxyz()),
                    std::forward_as_tuple(ck))
                .first->second);

        // While we are still holding our own lock, grab the lock of this chunk.
        SpinGuard chunkLock(holder.spin());
        cacheLock.unlock();

        // TODO For now we're only serializing at the end.
        // holder.init(0); // m_hierarchy.get(ck.dxyz()));
        return holder;
    }

    Hierarchy& m_hierarchy;
    const arbiter::Endpoint& m_out;
    const arbiter::Endpoint& m_tmp;

    SpinLock m_spin;
    std::map<Dxyz, Holder> m_chunks;
};

} // namespace entwine


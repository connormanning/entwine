/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/builder/chunk-cache.hpp>

#include <entwine/builder/hierarchy.hpp>
#include <entwine/builder/pruner.hpp>

namespace entwine
{

ChunkCache::ChunkCache(
        Hierarchy& hierarchy,
        Pool& ioPool,
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp)
    : m_hierarchy(hierarchy)
    , m_pool(ioPool)
    , m_out(out)
    , m_tmp(tmp)
{ }

ChunkCache::~ChunkCache()
{
    // TODO Remove.  Maybe assert.
    for (auto& slice : m_chunks)
    {
        if (slice.size()) std::cout << "CHUNKS REMAINING!!!" << std::endl;

        for (auto& p : slice)
        {
            NewChunk& c(*p.second);
            const uint64_t np = c.save(m_out, m_tmp);
            m_hierarchy.set(c.chunkKey().get(), np);
        }
    }
}

void ChunkCache::insert(
        Voxel& voxel,
        Key& key,
        const ChunkKey& ck,
        Pruner& pruner)
{
    // Get from single-threaded cache if we can.
    NewChunk* chunk = pruner.get(ck);

    // Otherwise, make sure it's initialized and increment its ref count.
    if (!chunk)
    {
        chunk = &addRef(ck, pruner);
        pruner.set(ck, chunk);
    }

    // Try to insert the point into this chunk.
    if (chunk->insert(*this, pruner, voxel, key)) return;

    // Failed to insert - need to traverse to the next depth.
    key.step(voxel.point());
    const Dir dir(getDirection(ck.bounds().mid(), voxel.point()));
    insert(voxel, key, chunk->childAt(dir), pruner);
}

NewChunk& ChunkCache::addRef(const ChunkKey& ck, Pruner& pruner)
{
    // This is the first access of this chunk for a particular thread.
    std::cout << "Creating " << ck.dxyz() << std::endl;

    UniqueSpin sliceLock(m_spins[ck.depth()]);

    auto& slice(m_chunks[ck.depth()]);
    auto it(slice.find(ck.position()));
    if (it != slice.end())
    {
        // If we have the chunk, simply increment its ref count.
        NewChunk& chunk = *it->second;
        SpinGuard chunkLock(chunk.spin());
        chunk.addRef();
        return chunk;
    }

    // Otherwise, create the chunk and initialize its contents.
    NewChunk& chunk(
            *slice.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(ck.position()),
                std::forward_as_tuple(makeUnique<NewChunk>(ck, m_hierarchy)))
            .first->second);

    sliceLock.unlock();

    // Initialize with remote data if we're reawakening this chunk.  It's ok
    // if other threads are inserting here concurrently.
    if (const uint64_t np = m_hierarchy.get(ck.dxyz()))
    {
        std::cout << "\tReawaken: " << ck.dxyz() << std::endl;
        chunk.load(*this, pruner, m_out, m_tmp, np);
    }

    return chunk;
}

void ChunkCache::prune(uint64_t depth, const std::map<Xyz, NewChunk*>& stale)
{
    if (stale.empty()) return;

    std::cout << "\t\tPruning " << depth << ": " << stale.size() << std::endl;

    auto& slice(m_chunks[depth]);
    SpinGuard lock(m_spins[depth]);

    for (const auto& p : stale)
    {
        const auto& key(p.first);
        assert(slice.count(key));

        // TODO Remove.
        if (!slice.count(key)) throw std::runtime_error("Missing key");

        NewChunk& c(*slice[key]);

        // TODO Remove.
        if (c.refs() != 1)
        {
            std::cout << "\t\t\tInvalid refs: " <<
                key.toString(depth) << ": " << c.refs() << std::endl;
            throw std::runtime_error("Invalid refs");
        }

        if (!c.delRef())
        {
            std::cout << "\t\t\tMark: " << key.toString(depth) << std::endl;
            const uint64_t np = c.save(m_out, m_tmp);
            m_hierarchy.set(c.chunkKey().get(), np);
            slice.erase(key);

            /*
            m_pool.addFront([&]()
            {
                SpinGuard lock(m_spins[depth]);
                if (c.refs())
                {
                    std::cout << "Interrupted " << c.chunkKey().dxyz() <<
                        std::endl;
                    return;
                }
            });
            */
        }
    }
}

void ChunkCache::purge()
{
}

} // namespace entwine


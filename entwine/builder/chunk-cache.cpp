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
    if (!chunk) chunk = &addRef(ck, pruner);

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

    auto& slice(m_reffedChunks[ck.depth()]);
    auto it(slice.find(ck.position()));
    if (it != slice.end())
    {
        // TODO Remove these - only applies to single-threaded builds for
        // debugging.
        std::cout << "Already have: " << ck.dxyz() << std::endl;
        throw std::runtime_error("Already have this chunk");

        // If we have the chunk, simply increment its ref count.
        // TODO Check this logic against our deleting logic.
        NewReffedChunk& ref = it->second;
        SpinGuard chunkLock(ref.spin());

        // if (!ref.exists())

        pruner.set(ck, &ref.chunk());
        ref.add();
        // TODO Might the chunk be a nullptr here?
        return ref.chunk();
    }

    // Otherwise, create the chunk and initialize its contents.
    NewReffedChunk& ref(
            slice.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(ck.position()),
                std::forward_as_tuple(ck, m_hierarchy))
            .first->second);
    pruner.set(ck, &ref.chunk());

    sliceLock.unlock();

    // Initialize with remote data if we're reawakening this chunk.  It's ok
    // if other threads are inserting here concurrently, and we have already
    // added our reference so it won't be getting deleted.
    if (const uint64_t np = m_hierarchy.get(ck.dxyz()))
    {
        std::cout << "\tReawaken: " << ck.dxyz() << " " << np << std::endl;
        ref.chunk().load(*this, pruner, m_out, m_tmp, np);
    }

    return ref.chunk();
}

void ChunkCache::prune(uint64_t depth, const std::map<Xyz, NewChunk*>& stale)
{
    if (stale.empty()) return;

    std::cout << "\t\tPruning " << depth << ": " << stale.size() << std::endl;

    auto& slice(m_reffedChunks[depth]);

    for (const auto& p : stale)
    {
        UniqueSpin sliceLock(m_spins[depth]);

        const auto& key(p.first);
        assert(slice.count(key));

        // TODO Remove.
        if (!slice.count(key)) throw std::runtime_error("Missing key");

        NewReffedChunk& ref(slice.at(key));
        UniqueSpin chunkLock(ref.spin());

        // TODO Remove.
        if (ref.count() != 1)
        {
            std::cout << "\t\t\tInvalid refs: " <<
                key.toString(depth) << ": " << ref.count() << std::endl;
            throw std::runtime_error("Invalid refs");
        }

        if (!ref.del())
        {
            chunkLock.unlock();
            sliceLock.unlock();

            // We've unlocked our locks, now we'll queue an async task to
            // delete this chunk.  It's possible another thread will show up
            // in the meantime and add a ref, in which case the async task
            // needs to atomically no-op.
            m_pool.add([this, key, depth, &ref, &slice]()
            {
                // Reacquire both locks in order and see what we need to do.
                UniqueSpin sliceLock(m_spins[depth]);
                UniqueSpin chunkLock(ref.spin());

                if (ref.count())
                {
                    std::cout << "--- Interrupted " <<
                        ref.chunk().chunkKey().dxyz() << std::endl;
                    return;
                }

                std::cout << "\tSaving " << ref.chunk().chunkKey().dxyz() <<
                    std::endl;

                // TODO Release this lock during IO, making sure we won't break
                // the addRef logic.

                // The actual IO is expensive, so retain only the chunk lock.
                // sliceLock.unlock();

                const uint64_t np = ref.chunk().save(m_out, m_tmp);
                m_hierarchy.set(ref.chunk().chunkKey().get(), np);

                // sliceLock.lock();

                if (ref.count())
                {
                    std::cout << "--- Interrupted after save " <<
                        ref.chunk().chunkKey().dxyz() << std::endl;
                    return;
                }

                // Must unlock this before we erase it so the chunkLock
                // destructor isn't trying to unlock a dangling SpinLock.
                chunkLock.unlock();

                slice.erase(key);
            });
        }
    }
}

} // namespace entwine


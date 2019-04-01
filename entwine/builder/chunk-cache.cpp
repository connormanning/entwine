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
    m_pool.join();

    for (auto& slice : m_reffedChunks)
    {
        for (auto& p : slice)
        {
            NewReffedChunk& c(p.second);
            if (c.exists())
            {
                std::cout << "BAD: " << c.chunk().chunkKey().dxyz() << ": " <<
                    c.count() << std::endl;
            }
        }
    }

    // TODO Remove.  Maybe assert.
    /*
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
    */
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
    // std::cout << "Creating " << ck.dxyz() << std::endl;

    UniqueSpin sliceLock(m_spins[ck.depth()]);  // TODO Deadlock.

    auto& slice(m_reffedChunks[ck.depth()]);
    auto it(slice.find(ck.position()));

    if (it != slice.end())
    {
        // std::cout << "Already have: " << ck.dxyz() << std::endl;

        NewReffedChunk& ref = it->second;
        SpinGuard chunkLock(ref.spin());

        // std::cout << "FOUND " << ck.dxyz() << std::endl;

        // If we have the chunk and it hasn't been serialized, simply increment
        // its ref count.
        //
        // It's possible that we started waiting on this chunkLock right after
        // it started being serialized, in which case we have caught it before
        // it's erased, so our newly added ref will stop that from happening.
        if (ref.exists())
        {
            ref.add();
            pruner.set(ck, &ref.chunk());
        }
        else
        {
            ref.assign(ck, m_hierarchy);
            pruner.set(ck, &ref.chunk());

            assert(!ref.count());
            assert(ref.exists());

            // After we add our ref-count, we know this ref won't be erased
            // from the slice.
            ref.add();
            // TODO.
            sliceLock.unlock();

            const uint64_t np = m_hierarchy.get(ck.dxyz());
            assert(np);

            // std::cout << "\tReawaken sneaky: " << ck.dxyz() << " " << np << std::endl;
            ref.chunk().load(*this, pruner, m_out, m_tmp, np);
            // std::cout << "Snuck!" << std::endl;
        }

        // std::cout << "\tRet existing" << std::endl;

        return ref.chunk();
    }

    // std::cout << "DNE " << ck.dxyz() << std::endl;

    // Couldn't find this chunk, create it.
    auto insertion = slice.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(ck.position()),
            std::forward_as_tuple(ck, m_hierarchy));

    it = insertion.first;
    assert(insertion.second);

    NewReffedChunk& ref = it->second;
    SpinGuard chunkLock(ref.spin());

    // We shouldn't have any existing refs yet, but the chunk should exist.
    assert(!ref.count());
    assert(ref.exists());

    // Since we're still holding the slice lock, no one else can access this
    // chunk yet.  Add our ref and then we can release the slice lock.
    ref.add();
    pruner.set(ck, &ref.chunk());

    // TODO.
    sliceLock.unlock();

    // Initialize with remote data if we're reawakening this chunk.  It's ok
    // if other threads are inserting here concurrently, and we have already
    // added our reference so it won't be getting deleted.
    //
    // Note that this in the case of a continued build, this chunk may have
    // been serialized prior to the current build process, so we still need to
    // check this.
    if (const uint64_t np = m_hierarchy.get(ck.dxyz()))
    {
        std::cout << "\tReawaken prior: " << ck.dxyz() << " " << np <<
            std::endl;
        ref.chunk().load(*this, pruner, m_out, m_tmp, np);
    }

    // std::cout << "LOADED " << ck.dxyz() << std::endl;

    return ref.chunk();
}

void ChunkCache::prune(uint64_t depth, const std::map<Xyz, NewChunk*>& stale)
{
    if (stale.empty()) return;

    // std::cout << "\t\tPruning " << depth << ": " << stale.size() << std::endl;

    auto& slice(m_reffedChunks[depth]);

    for (const auto& p : stale)
    {
        UniqueSpin sliceLock(m_spins[depth]);

        const auto& key(p.first);
        assert(slice.count(key));

        NewReffedChunk& ref(slice.at(key));
        UniqueSpin chunkLock(ref.spin());

        // std::cout << "REFS " << key.toString(depth) << " " << ref.count() << std::endl;

        assert(ref.count());

        // std::cout << "Pruning " << key.toString(depth) << std::endl;
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
                UniqueSpin chunkLock(ref.spin());   // TODO Deadlock.

                if (ref.count())
                {
                    // In the time between queueing this serialization and its
                    // execution, another thread has claimed this chunk.  No-op.
                    std::cout << "--- Interrupted " <<
                        key.toString(depth) << std::endl;
                    return;
                }

                if (!ref.exists())
                {
                    // In this time between queueing this serialization and its
                    // execution, another thread has erased this chunk.  For
                    // example:
                    //      Queue for serialization
                    //      Reclaim before serialization occurs
                    //      Queue for serialization again
                    //
                    // This is ok but should be rare, since we don't search our
                    // serialization queue and remove entries when they're
                    // reclaimed.  Instead we just wait for them to execute at
                    // which time they'll simply no-op here.
                    std::cout << "--- Second interrupt " <<
                        key.toString(depth) << std::endl;
                    return;
                }

                // std::cout << "DEL " << key.toString(depth) << std::endl;

                // std::cout << "\tSaving " << key.toString(depth) << std::endl;

                // The actual IO is expensive, so retain only the chunk lock.
                // Note: As soon as we let go of the slice lock, another thread
                // could arrive and be waiting for this chunk lock.
                //
                // Once we unlock this, someone could grab this sliceLock and
                // be waiting on our chunkLock.
                // TODO.
                sliceLock.unlock();

                // At this point, another thread could arrive and be waiting on
                // the chunk lock, which we are holding.

                // TODO Why might ref.chunk() not exist here?
                const uint64_t np = ref.chunk().save(m_out, m_tmp);
                m_hierarchy.set(ref.chunk().chunkKey().get(), np);

                // Cannot erase this chunk here, since we haven't been holding
                // the sliceLock, someone may be waiting for this chunkLock.
                // Instead we'll just reset the pointer.
                //
                // If another thread *is* waiting for this lock, they'll
                // initialize it and set the ref count to non-zero, in which
                // case our upcoming locking will be for no reason.
                //
                // Usually no one will be waiting for this chunk, so it's ok.
                ref.reset();
                // std::cout << "\tSAVED" << std::endl;

                if (ref.count())
                {
                    std::cout << "COUNT " << key.toString(depth) << std::endl;
                    throw std::runtime_error("Inv count after save");
                }

                // TODO.
                return;

                chunkLock.unlock();

                // Must acquire these in this order.
                sliceLock.lock();
                chunkLock.lock();

                if (!ref.count())
                {
                    if (!ref.exists())
                    {
                        std::cout << "Still exists: " << key.toString(depth) <<
                            std::endl;
                        throw std::runtime_error("Still exists!!!");
                    }
                    assert(!ref.exists());

                    // Necessary to avoid the chunkLock trying to clear the
                    // underlying SpinLock (which we're about to erase) on its
                    // destruction.
                    //
                    // Because we have both locks, we know that no one is
                    // waiting on this chunkLock.
                    chunkLock.release();
                    slice.erase(key);
                }
                else std::cout << "Late interrupt!" << std::endl;
            });
        }
    }
}

} // namespace entwine


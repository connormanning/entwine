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

    assert(
            std::all_of(
                m_reffedChunks.begin(),
                m_reffedChunks.end(),
                [](const std::map<Xyz, NewReffedChunk>& slice)
                {
                    return slice.empty();
                }));
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
    UniqueSpin sliceLock(m_spins[ck.depth()]);

    auto& slice(m_reffedChunks[ck.depth()]);
    auto it(slice.find(ck.position()));

    if (it != slice.end())
    {
        NewReffedChunk& ref = it->second;
        SpinGuard chunkLock(ref.spin());

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

            sliceLock.unlock();

            const uint64_t np = m_hierarchy.get(ck.dxyz());
            assert(np);

            {
                SpinGuard lock(m_infoSpin);
                ++m_info.read;
            }

            ref.chunk().load(*this, pruner, m_out, m_tmp, np);
        }

        return ref.chunk();
    }

    // Couldn't find this chunk, create it.
    auto insertion = slice.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(ck.position()),
            std::forward_as_tuple(ck, m_hierarchy));

    {
        SpinGuard lock(m_infoSpin);
        ++m_info.alive;
    }

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
        {
            SpinGuard lock(m_infoSpin);
            ++m_info.read;
        }

        ref.chunk().load(*this, pruner, m_out, m_tmp, np);
    }

    return ref.chunk();
}

void ChunkCache::prune(uint64_t depth, const std::map<Xyz, NewChunk*>& stale)
{
    if (stale.empty()) return;

    auto& slice(m_reffedChunks[depth]);
    UniqueSpin sliceLock(m_spins[depth]);

    for (const auto& p : stale)
    {
        const auto& key(p.first);
        assert(slice.count(key));

        NewReffedChunk& ref(slice.at(key));
        UniqueSpin chunkLock(ref.spin());

        assert(ref.count());
        if (!ref.del())
        {
            // Once we've unreffed this chunk, all bets are off as to its
            // validity.  It may be recaptured before deletion by an insertion
            // thread, or may be deleted instantly.
            chunkLock.unlock();
            sliceLock.unlock();

            const Dxyz dxyz(depth, key);
            m_pool.add([this, dxyz]() { maybeSerialize(dxyz); });

            sliceLock.lock();
        }
    }
}

void ChunkCache::maybeSerialize(const Dxyz& dxyz)
{
    // Acquire both locks in order and see what we need to do.
    UniqueSpin sliceLock(m_spins[dxyz.depth()]);
    auto& slice(m_reffedChunks[dxyz.depth()]);
    auto it(slice.find(dxyz.position()));

    if (it == slice.end())
    {
        std::cout << "--- Interrupted: erased " << dxyz << std::endl;
        return;
    }

    NewReffedChunk& ref = it->second;
    UniqueSpin chunkLock(ref.spin());

    if (ref.count())
    {
        std::cout << "--- Interrupted: reclaimed " << dxyz << std::endl;
        return;
    }

    if (!ref.exists())
    {
        std::cout << "--- Interrupted: serialized " << dxyz << std::endl;
        return;
    }

    // At this point, we have both locks, and we know our chunk exists but has
    // no refs, so serialize it.
    //
    // The actual IO is expensive, so retain only the chunk lock.  Note: As
    // soon as we let go of the slice lock, another thread could arrive and be
    // waiting for this chunk lock, so we can't delete the ref from our map
    // outright after this point without reclaiming the locks.
    sliceLock.unlock();

    assert(ref.exists());

    {
        SpinGuard lock(m_infoSpin);
        ++m_info.written;
    }

    const uint64_t np = ref.chunk().save(m_out, m_tmp);
    m_hierarchy.set(ref.chunk().chunkKey().get(), np);
    assert(np);

    // Cannot erase this chunk here, since we haven't been holding the
    // sliceLock, someone may be waiting for this chunkLock.  Instead we'll
    // just reset the pointer.  We'll have to reacquire both locks to attempt
    // to erase it.
    ref.reset();
    chunkLock.unlock();

    maybeErase(dxyz);
}

void ChunkCache::maybeErase(const Dxyz& dxyz)
{
    UniqueSpin sliceLock(m_spins[dxyz.depth()]);
    auto& slice(m_reffedChunks[dxyz.depth()]);
    auto it(slice.find(dxyz.position()));

    if (it == slice.end())
    {
        std::cout << "--- Interrupted: prior to erase" << dxyz << std::endl;
        return;
    }

    NewReffedChunk& ref = it->second;
    UniqueSpin chunkLock(ref.spin());

    if (ref.count())
    {
        std::cout << "--- Erase late interrupt: " << dxyz << std::endl;
        return;
    }

    if (ref.exists())
    {
        std::cout << "--- Still exists during maybeErase: " << dxyz << std::endl;
        return;
    }

    // Because we have both locks, we know that no one is waiting on this chunk.
    //
    // Release the chunkLock so the unique_lock doesn't try to unlock a deleted
    // SpinLock when it destructs.
    chunkLock.release();
    slice.erase(it);

    {
        SpinGuard lock(m_infoSpin);
        --m_info.alive;
    }
}

} // namespace entwine


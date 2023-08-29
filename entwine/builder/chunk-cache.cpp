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

#include <entwine/builder/clipper.hpp>

namespace entwine
{

namespace
{
    SpinLock infoSpin;
    ChunkCache::Info info;
}

ChunkCache::Info ChunkCache::latchInfo()
{
    SpinGuard lock(infoSpin);
    Info latched = info;
    info.written = 0;
    info.read = 0;
    return latched;
}

ChunkCache::ChunkCache(
    const Endpoints& endpoints,
    const Metadata& metadata,
    Hierarchy& hierarchy,
    const uint64_t threads)
    : m_endpoints(endpoints)
    , m_metadata(metadata)
    , m_hierarchy(hierarchy)
    , m_pool(threads)
{ }

ChunkCache::~ChunkCache()
{
    join();
}

void ChunkCache::join()
{
    maybePurge(0);
    m_pool.join();

    assert(
        std::all_of(
            m_slices.begin(),
            m_slices.end(),
            [](const std::map<Xyz, ReffedChunk>& slice)
            {
                return slice.empty();
            }));
}

bool ChunkCache::insert(
        Voxel& voxel,
        Key& key,
        const ChunkKey& ck,
        Clipper& clipper)
{
    // This point is likely one of several thousand points with exactly
    // duplicated XYZ values - discard it.
    if (ck.depth() >= maxDepth) return false;

    // Get from single-threaded cache if we can.
    Chunk* chunk = clipper.get(ck);

    // Otherwise, make sure it's initialized and increment its ref count.
    if (!chunk) chunk = &addRef(ck, clipper);

    // Try to insert the point into this chunk.
    if (chunk->insert(*this, clipper, voxel, key)) return true;

    // Failed to insert - need to traverse to the next depth.
    key.step(voxel.point());
    const Dir dir(getDirection(ck.bounds().mid(), voxel.point()));
    return insert(voxel, key, chunk->childAt(dir), clipper);
}

Chunk& ChunkCache::addRef(const ChunkKey& ck, Clipper& clipper)
{
    // This is the first access of this chunk for a particular thread.
    UniqueSpin sliceLock(m_spins[ck.depth()]);

    auto& slice(m_slices[ck.depth()]);
    auto it(slice.find(ck.position()));

    if (it != slice.end())
    {
        // We've found a reffed chunk here.  The chunk itself may not exist,
        // since the serialization and deletion steps occur asynchronously.
        ReffedChunk& ref = it->second;
        UniqueSpin chunkLock(ref.spin());
        ref.add();

        sliceLock.unlock();

        if (!ref.exists())
        {
            assert(ref.count() == 1);

            // This chunk has already been serialized, but we've caught hold of
            // its lock before it was actually erased from our map.  In this
            // case, we'll need to reinitialize the resident chunk from its
            // remote source.  Our newly added reference will keep it from
            // being erased.
            ref.assign(m_metadata, ck, m_hierarchy);
            assert(ref.exists());

            {
                SpinGuard lock(infoSpin);
                ++info.read;
            }

            const uint64_t np = hierarchy::get(m_hierarchy, ck.dxyz());
            assert(np);

            // Need to insert this ref prior to loading the chunk or we'll end
            // up deadlocked.
            clipper.set(ck, &ref.chunk());
            ref.chunk().load(*this, clipper, m_endpoints, np);
        }
        else clipper.set(ck, &ref.chunk());

        chunkLock.unlock();

        // If we've reclaimed this chunk while it sits in our ownership list,
        // remove it from that list - it is now communally owned.
        SpinGuard ownedLock(m_ownedSpin);
        auto it(m_owned.find(ck.dxyz()));
        if (it != m_owned.end())
        {
            chunkLock.lock();
            assert(ref.count() > 1);
            ref.del();
            m_owned.erase(it);
        }

        return ref.chunk();
    }

    // Couldn't find this chunk, create it.
    auto insertion = slice.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(ck.position()),
            std::forward_as_tuple(m_metadata, ck, m_hierarchy));

    {
        SpinGuard lock(infoSpin);
        ++info.alive;
    }

    it = insertion.first;
    assert(insertion.second);

    ReffedChunk& ref = it->second;
    SpinGuard chunkLock(ref.spin());

    // We shouldn't have any existing refs yet, but the chunk should exist.
    assert(!ref.count());
    assert(ref.exists());

    // Since we're still holding the slice lock, no one else can access this
    // chunk yet.  Add our ref and then we can release the slice lock.
    ref.add();
    clipper.set(ck, &ref.chunk());

    sliceLock.unlock();

    // Initialize with remote data if we're reawakening this chunk.  It's ok
    // if other threads are inserting here concurrently, and we have already
    // added our reference so it won't be getting deleted.
    //
    // Note that this in the case of a continued build, this chunk may have
    // been serialized prior to the current build process, so we still need to
    // check this.
    if (const uint64_t np = hierarchy::get(m_hierarchy, ck.dxyz()))
    {
        {
            SpinGuard lock(infoSpin);
            ++info.read;
        }

        ref.chunk().load(*this, clipper, m_endpoints, np);
    }

    return ref.chunk();
}

void ChunkCache::clip(uint64_t depth, const std::map<Xyz, Chunk*>& stale)
{
    if (stale.empty()) return;

    auto& slice(m_slices[depth]);
    UniqueSpin sliceLock(m_spins[depth]);

    for (const auto& p : stale)
    {
        const auto& key(p.first);
        assert(slice.count(key));

        ReffedChunk& ref(slice.at(key));
        UniqueSpin chunkLock(ref.spin());

        assert(ref.count());
        if (!ref.del())
        {
            // Defer erasing here, instead adding taking ownership.
            ref.add();

            chunkLock.unlock();
            sliceLock.unlock();

            {
                SpinGuard ownedLock(m_ownedSpin);
                const Dxyz dxyz(depth, key);
                assert(!m_owned.count(dxyz));
                m_owned.insert(dxyz);
            }

            sliceLock.lock();
        }
    }
}

void ChunkCache::maybePurge(const uint64_t maxCacheSize)
{
    uint64_t disowned(0);
    UniqueSpin ownedLock(m_ownedSpin);
    while (m_owned.size() > maxCacheSize)
    {
        ++disowned;

        const Dxyz dxyz(*m_owned.rbegin());
        auto& slice(m_slices[dxyz.depth()]);
        UniqueSpin sliceLock(m_spins[dxyz.depth()]);

        ReffedChunk& ref(slice.at(dxyz.position()));
        UniqueSpin chunkLock(ref.spin());

        m_owned.erase(std::prev(m_owned.end()));

        // If we're destructing and thus purging everything, we should be the
        // only ref-holder.
        assert(maxCacheSize || ref.count() == 1);

        if (!ref.del())
        {
            // Once we've unreffed this chunk, all bets are off as to its
            // validity.  It may be recaptured before deletion by an insertion
            // thread, or may be deleted instantly.
            chunkLock.unlock();
            sliceLock.unlock();
            ownedLock.unlock();

            // Don't hold any locks while we do this, since it may block.  We
            // only want to block the calling thread in this case, not the
            // whole system.
            m_pool.add([this, dxyz]() { maybeSerialize(dxyz); });

            ownedLock.lock();
        }
    }
}

void ChunkCache::maybeSerialize(const Dxyz& dxyz)
{
    // Acquire both locks in order and see what we need to do.
    UniqueSpin sliceLock(m_spins[dxyz.depth()]);
    auto& slice(m_slices[dxyz.depth()]);
    auto it(slice.find(dxyz.position()));

    // This case represents a chunk that has been queued for serialization,
    // then reclaimed, and then queued for serialization again.  If the first
    // serialization request doesn't actually run until after these steps,
    // we'll end up with two serialization requests in the queue at which point
    // the second one should simply no-op.
    //
    // This check keeps us from having to search our serialization queue for
    // cleanup every time a chunk is reclaimed prior to its async serialization.
    if (it == slice.end()) return;

    ReffedChunk& ref = it->second;
    UniqueSpin chunkLock(ref.spin());

    // This chunk was queued for serialization, but another thread arrived to
    // claim it before the serialization occurred.  No-op.
    if (ref.count()) return;

    // This case occurs during the double-serialization case described above,
    // when the second serialization shows up to wait on the chunk lock while
    // the first serialization occurs.  The chunk is serialized and reset by
    // the first thread, but it has to reacquire both locks in the proper order
    // to avoid deadlock before it is actually removed.  If we've slipped in
    // during this reacquisition time, simply no-op.  The first thread will
    // erase the chunk immediately after we release the lock here.
    if (!ref.exists()) return;

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
        SpinGuard lock(infoSpin);
        ++info.written;
    }

    const uint64_t np = ref.chunk().save(m_endpoints);
    hierarchy::set(m_hierarchy, ref.chunk().chunkKey().get(), np);
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
    auto& slice(m_slices[dxyz.depth()]);
    auto it(slice.find(dxyz.position()));

    // If the chunk has already been erased, no-op.
    if (it == slice.end()) return;

    ReffedChunk& ref = it->second;
    UniqueSpin chunkLock(ref.spin());

    if (ref.count()) return;
    if (ref.exists()) return;

    // Because we have both locks, we know that no one is waiting on this chunk.
    //
    // Release the chunkLock so the unique_lock doesn't try to unlock a deleted
    // SpinLock when it destructs.
    chunkLock.release();
    slice.erase(it);

    {
        SpinGuard lock(infoSpin);
        --info.alive;
    }
}

} // namespace entwine

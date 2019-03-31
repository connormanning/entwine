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

namespace entwine
{

ChunkCache::ChunkCache(
        Hierarchy& hierarchy,
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp)
    : m_hierarchy(hierarchy)
    , m_out(out)
    , m_tmp(tmp)
{ }

ChunkCache::~ChunkCache()
{
    for (auto& p : m_chunks)
    {
        NewChunk& c(p.second);
        const uint64_t np = c.save(m_out, m_tmp);
        m_hierarchy.set(c.chunkKey().get(), np);
    }
}

void ChunkCache::insert(
        Voxel& voxel,
        Key& key,
        const ChunkKey& ck,
        Pruner& pruner,
        bool force)
{
    // Get from single-threaded cache if we can.
    NewChunk* chunk = pruner.get(ck);

    // Otherwise, make sure it's initialized and increment its ref count.
    if (!chunk)
    {
        chunk = &addRef(ck);
        pruner.set(ck, chunk);
    }

    // Try to insert the point into this chunk.
    if (chunk->insert(*this, pruner, voxel, key)) return;

    // Failed to insert - need to traverse to the next depth.
    key.step(voxel.point());
    const Dir dir(getDirection(ck.bounds().mid(), voxel.point()));
    insert(voxel, key, chunk->childAt(dir), pruner);
}

NewChunk& ChunkCache::addRef(const ChunkKey& ck)
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
        NewChunk& chunk = it->second;
        SpinGuard chunkLock(chunk.spin());
        chunk.addRef();
        return chunk;
    }

    // Otherwise, create the chunk and initialize its contents.
    NewChunk& chunk(
            m_chunks.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(ck.dxyz()),
                std::forward_as_tuple(ck))
            .first->second);

    // While we are still holding our own lock, grab the lock of this chunk.
    SpinGuard chunkLock(chunk.spin());
    cacheLock.unlock();

    // TODO Potentially initialize with remote data, determined by:
    //      m_hierarchy.get(ck.dxyz()));

    return chunk;
}

} // namespace entwine


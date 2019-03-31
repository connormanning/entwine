/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

#include <entwine/builder/chunk.hpp>
#include <entwine/builder/chunk-cache.hpp>
#include <entwine/builder/clipper.hpp>
#include <entwine/builder/hierarchy.hpp>
#include <entwine/builder/thread-pools.hpp>
#include <entwine/types/key.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class Clipper;

class Registry
{
public:
    Registry(
            const Metadata& metadata,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            ThreadPools& threadPools,
            bool exists = false);

    void save();
    void merge(const Registry& other, Clipper& clipper);

    void addPoint(Voxel& voxel, Key& key, Clipper& clipper)
    {
        m_root.insert(voxel, key, clipper);
    }

    void newAddPoint(Voxel& voxel, Key& key, ChunkKey& ck, Pruner& pruner)
    {
        m_chunkCache->insert(voxel, key, ck, pruner);
    }

    void purge() { m_root.empty(); }

    Pool& workPool() { return m_threadPools.workPool(); }
    Pool& clipPool() { return m_threadPools.clipPool(); }

    const Metadata& metadata() const { return m_metadata; }
    const Hierarchy& hierarchy() const { return m_hierarchy; }

private:
    const Metadata& m_metadata;
    const arbiter::Endpoint m_dataEp;
    const arbiter::Endpoint m_hierEp;
    const arbiter::Endpoint& m_tmp;
    ThreadPools& m_threadPools;
    Hierarchy m_hierarchy;

    ReffedChunk m_root;
    std::unique_ptr<ChunkCache> m_chunkCache;
};

} // namespace entwine


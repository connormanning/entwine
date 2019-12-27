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

#include <entwine/builder/chunk-cache.hpp>
#include <entwine/builder/hierarchy.hpp>
#include <entwine/builder/thread-pools.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/key.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class Registry
{
public:
    Registry(
            const Metadata& metadata,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            uint64_t clipThreads,
            bool exists = false);

    void save(uint64_t hierarchyStep, uint64_t threads, bool verbose);
    void merge(
        const Registry& other,
        Clipper& clipper,
        const arbiter::Endpoint& tmp);

    void addPoint(Voxel& voxel, Key& key, ChunkKey& ck, Clipper& clipper)
    {
        m_chunkCache->insert(voxel, key, ck, clipper);
    }

    const Metadata& metadata() const { return m_metadata; }
    const Hierarchy& hierarchy() const { return m_hierarchy; }
    ChunkCache& cache() const { return *m_chunkCache; }

private:
    const Metadata& m_metadata;
    const arbiter::Endpoint m_dataEp;
    const arbiter::Endpoint m_hierEp;
    Hierarchy m_hierarchy;

    std::unique_ptr<ChunkCache> m_chunkCache;
};

} // namespace entwine


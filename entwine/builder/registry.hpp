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

#include <json/json.h>

#include <entwine/builder/chunk.hpp>
#include <entwine/builder/clipper.hpp>
#include <entwine/builder/hierarchy.hpp>
#include <entwine/builder/thread-pools.hpp>
#include <entwine/types/key.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/tube.hpp>
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
            PointPool& pointPool,
            ThreadPools& threadPools,
            bool exists = false);

    void save(const arbiter::Endpoint& endpoint) const;
    void merge(const Registry& other, Clipper& clipper);

    bool addPoint(Cell::PooledNode& cell, Key& key, Clipper& clipper)
    {
        ReffedChunk* rc = &m_root;

        while (!rc->insert(cell, key, clipper))
        {
            key.step(cell->point());
            rc = &rc->chunk().step(cell->point());
        }

        return true;
    }

    void purge() { m_root.empty(); }

    Pool& workPool() { return m_threadPools.workPool(); }
    Pool& clipPool() { return m_threadPools.clipPool(); }

    const Metadata& metadata() const { return m_metadata; }
    const Hierarchy& hierarchy() const { return m_hierarchy; }

private:
    const Metadata& m_metadata;
    const arbiter::Endpoint& m_out;
    const arbiter::Endpoint& m_tmp;
    PointPool& m_pointPool;
    ThreadPools& m_threadPools;
    Hierarchy m_hierarchy;

    ReffedChunk m_root;
};

} // namespace entwine


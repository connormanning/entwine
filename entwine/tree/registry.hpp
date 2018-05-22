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

#include <entwine/tree/new-clipper.hpp>
#include <entwine/tree/cold.hpp>
#include <entwine/tree/hierarchy.hpp>
#include <entwine/tree/self-chunk.hpp>
#include <entwine/tree/slice.hpp>
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

class NewClimber;
class NewClipper;

class Registry
{
public:
    Registry(
            const Metadata& metadata,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pointPool,
            Pool& threadPool,
            bool exists = false);

    void save(const arbiter::Endpoint& endpoint) const;
    void merge(const Registry& other, NewClipper& clipper);

    bool addPoint(Cell::PooledNode& cell, Key& key, NewClipper& clipper)
    {
        ReffedFixedChunk* rc = &m_root;

        while (!rc->insert(cell, key, clipper))
        {
            key.step(cell->point());
            rc = &rc->chunk().step(cell->point());
        }

        return true;
    }

    void purge()
    {
        m_root.empty();
    }

    void clip(uint64_t d, const Xyz& p, uint64_t o)
    {
        m_threadPool.add([this, d, p, o] { m_slices.at(d).clip(p, o); });
    }

    Pool& clipPool() { return m_threadPool; }

    const Metadata& metadata() const { return m_metadata; }
    const Hierarchy& hierarchy() const { return m_hierarchy; }

private:
    void loadAsNew();
    void loadFromRemote();

    void hierarchy(
            Json::Value& json,
            uint64_t d,
            Xyz p,
            uint64_t maxDepth = 0) const;

    const Metadata& m_metadata;
    const arbiter::Endpoint& m_out;
    const arbiter::Endpoint& m_tmp;
    PointPool& m_pointPool;
    Pool& m_threadPool;
    Hierarchy m_hierarchy;

    std::vector<Slice> m_slices;
    ReffedFixedChunk m_root;
};

} // namespace entwine


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
#include <cstdint>
#include <set>

#include <json/json.h>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/hierarchy-block.hpp>
#include <entwine/tree/splitter.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/spin-lock.hpp>

namespace entwine
{

class Pool;
class PointState;
class Subset;

class Hierarchy : public Splitter<HierarchyBlock>
{
public:
    Hierarchy(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const arbiter::Endpoint& top,
            const arbiter::Endpoint* topOut,
            bool exists);

    ~Hierarchy()
    {
        m_base.t.reset();

        iterateCold([this](const Id& chunkId, std::size_t num, const Slot& slot)
        {
            // We need to release our pooled block nodes back into our pool
            // so the parent destructor doesn't release them into a stale pool.
            slot.t.reset();
        });
    }

    using Splitter::tryGet;

    void countBase(std::size_t index, std::size_t tick, int delta)
    {
        m_base.t->count(index, tick, delta);
    }

    HierarchyCell& count(const PointState& state, int delta);
    HierarchyCell& count(
            const ChunkInfo& chunkInfo,
            std::size_t tick,
            int delta);

    uint64_t tryGet(const PointState& pointState) const;

    void save(Pool& pool) const;
    void awakenAll(Pool& pool) const;
    void merge(Hierarchy& other, Pool& pool);

    static Structure structure(
            const Structure& treeStructure,
            const Subset* subset = nullptr);

    using Slots = std::set<const Slot*>;

protected:
    HierarchyCell::Pool& m_pool;
    const Metadata& m_metadata;
    const Bounds& m_bounds;
    const Structure& m_structure;
    const arbiter::Endpoint m_endpoint;
    const std::unique_ptr<arbiter::Endpoint> m_outpoint;
};

} // namespace entwine


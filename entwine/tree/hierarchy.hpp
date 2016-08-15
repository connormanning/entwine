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
#include <deque>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include <entwine/third/json/json.hpp>
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
            bool exists = false);

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

    uint64_t tryGet(const PointState& pointState) const;

    void save(Pool& pool);
    void awakenAll(Pool& pool) const;
    void merge(Hierarchy& other, Pool& pool);

    using Slots = std::set<const Slot*>;
    struct QueryResults
    {
        Json::Value json;
        Slots touched;
    };

    QueryResults query(
            const Bounds& queryBounds,
            std::size_t depthBegin,
            std::size_t depthEnd);

    QueryResults queryVertical(
            const Bounds& queryBounds,
            std::size_t depthBegin,
            std::size_t depthEnd);

    static Structure structure(
            const Structure& treeStructure,
            const Subset* subset = nullptr);

private:
    class Query
    {
    public:
        Query(
                const Bounds& bounds,
                std::size_t depthBegin,
                std::size_t depthEnd)
            : m_bounds(bounds)
            , m_depthBegin(depthBegin)
            , m_depthEnd(depthEnd)
        { }

        const Bounds& bounds() const { return m_bounds; }
        std::size_t depthBegin() const { return m_depthBegin; }
        std::size_t depthEnd() const { return m_depthEnd; }

    private:
        const Bounds m_bounds;
        const std::size_t m_depthBegin;
        const std::size_t m_depthEnd;
    };

    void traverse(
            Json::Value& json,
            Slots& ids,
            const Query& query,
            const PointState& pointState,
            std::deque<Dir>& lag);

    void accumulate(
            Json::Value& json,
            Slots& ids,
            const Query& query,
            const PointState& pointState,
            std::deque<Dir>& lag,
            uint64_t inc);

    void reduce(
            std::vector<std::size_t>& out,
            std::size_t depth,
            const Json::Value& in) const;

    void maybeTouch(Slots& ids, const PointState& pointState) const;

    HierarchyCell::Pool& m_pool;
    const Metadata& m_metadata;
    const Bounds& m_bounds;
    const Structure& m_structure;
    const arbiter::Endpoint m_endpoint;
    const std::unique_ptr<arbiter::Endpoint> m_outpoint;
};

} // namespace entwine


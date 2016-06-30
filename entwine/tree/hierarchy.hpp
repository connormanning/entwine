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

class PointState;

class Hierarchy : public Splitter<HierarchyBlock>
{
public:
    Hierarchy(
            const Metadata& metadata,
            const arbiter::Endpoint& ep,
            bool exists = false);

    using Splitter::tryGet;

    void count(const PointState& state, int delta);
    uint64_t tryGet(const PointState& pointState) const;

    void save(const arbiter::Endpoint& top)
    {
        const auto ep(top.getSubEndpoint("h"));
        const std::string postfix(m_metadata.postfix());

        m_base.t->save(ep, postfix);

        iterateCold([&ep, postfix](const Id& chunkId, const Slot& slot)
        {
            if (slot.t) slot.t->save(ep, postfix);
        });

        Json::Value json;
        for (const auto& id : ids()) json.append(id.str());
        ep.put("ids" + postfix, toFastString(json));
    }

    void awakenAll()
    {
        iterateCold([this](const Id& chunkId, const Slot& slot)
        {
            slot.t = HierarchyBlock::create(
                    m_structure,
                    chunkId,
                    m_structure.getInfo(chunkId).pointsPerChunk(),
                    m_endpoint.getBinary(chunkId.str()));
        });
    }

    void merge(const Hierarchy& other)
    {
        dynamic_cast<ContiguousBlock&>(*m_base.t).merge(
                dynamic_cast<const ContiguousBlock&>(*other.m_base.t));

        Splitter::merge(other.ids());
    }

    Json::Value query(
            const Bounds& queryBounds,
            std::size_t depthBegin,
            std::size_t depthEnd);

    static Structure structure(const Structure& treeStructure);

private:
    class Query
    {
    public:
        Query(const Bounds& bounds, std::size_t depthBegin, std::size_t depthEnd)
            : m_bounds(bounds), m_depthBegin(depthBegin), m_depthEnd(depthEnd)
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
            const Query& query,
            const PointState& pointState,
            std::deque<Dir>& lag);

    void accumulate(
            Json::Value& json,
            const Query& query,
            const PointState& pointState,
            std::deque<Dir>& lag,
            uint64_t inc);

    const Metadata& m_metadata;
    const Bounds& m_bounds;
    const Structure& m_structure;
    const arbiter::Endpoint m_endpoint;
};

} // namespace entwine


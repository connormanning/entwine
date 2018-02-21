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
/*

#include <cstddef>
#include <list>
#include <set>
#include <unordered_map>
#include <vector>

#include <entwine/tree/heuristics.hpp>
#include <entwine/tree/registry.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class Clipper
{
private:
    struct ClipInfo
    {
        using Map = std::map<Id, ClipInfo>;
        using Order = std::list<Map::iterator>;

        ClipInfo() : chunkNum(0), fresh(true), orderIt() { }

        explicit ClipInfo(std::size_t chunkNum)
            : chunkNum(chunkNum)
            , fresh(true)
            , orderIt()
        { }

        std::size_t chunkNum;
        bool fresh;
        std::unique_ptr<Order::iterator> orderIt;
    };

public:
    Clipper(Builder& builder, Origin origin)
        : m_builder(builder)
        // , m_startDepth(builder.metadata().structure().coldDepthBegin())
        , m_startDepth(0)
        , m_id(origin)
        , m_clips()
        , m_fastCache(32, m_clips.end())
        , m_order()
    { }

    ~Clipper()
    {
        for (const auto& c : m_clips)
        {
            // m_builder.clip(c.first, c.second.chunkNum, m_id);
        }
    }

    bool insert(const Id& chunkId, std::size_t chunkNum, std::size_t depth);

    void clip()
    {
        if (m_clips.size() < heuristics::clipCacheSize) return;

        m_fastCache.assign(32, m_clips.end());
        bool done(false);

        while (m_clips.size() > heuristics::clipCacheSize && !done)
        {
            ClipInfo::Map::iterator& it(*m_order.rbegin());

            if (!it->second.fresh)
            {
                // m_builder.clip(it->first, it->second.chunkNum, m_id);
                m_clips.erase(it);
                m_order.pop_back();
            }
            else
            {
                done = true;
            }
        }

        for (auto& p : m_clips) p.second.fresh = false;
    }

    void clip(const Id& chunkId)
    {
        // m_builder.clip(chunkId, m_clips.at(chunkId).chunkNum, m_id, true);
        m_clips.erase(chunkId);
    }

    std::size_t id() const { return m_id; }
    std::size_t size() const { return m_clips.size(); }

private:
    Builder& m_builder;
    const std::size_t m_startDepth;
    const uint64_t m_id;

    ClipInfo::Map m_clips;
    std::vector<ClipInfo::Map::iterator> m_fastCache;

    ClipInfo::Order m_order;
};

} // namespace entwine
*/


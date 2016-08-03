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
#include <list>
#include <set>
#include <unordered_map>
#include <vector>

#include <entwine/tree/builder.hpp>
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
        , m_startDepth(builder.metadata().structure().coldDepthBegin())
        , m_id(origin)
        , m_clips()
        , m_fastCache(32, m_clips.end())
        , m_order()
    { }

    ~Clipper()
    {
        for (const auto& c : m_clips)
        {
            m_builder.clip(c.first, c.second.chunkNum, m_id);
        }
    }

    bool insert(const Id& chunkId, std::size_t chunkNum, std::size_t depth)
    {
        assert(depth >= m_startDepth);
        depth -= m_startDepth;

        if (depth < m_fastCache.size())
        {
            auto& it(m_fastCache[depth]);

            if (it != m_clips.end() && it->first == chunkId)
            {
                it->second.fresh = true;
                m_order.splice(
                        m_order.begin(),
                        m_order,
                        *it->second.orderIt);

                return false;
            }
        }

        auto it(m_clips.find(chunkId));

        if (it != m_clips.end())
        {
            if (depth < m_fastCache.size()) m_fastCache[depth] = it;

            it->second.fresh = true;
            m_order.splice(
                    m_order.begin(),
                    m_order,
                    *it->second.orderIt);

            return false;
        }
        else
        {
            it = m_clips.insert(
                        std::make_pair(chunkId, ClipInfo(chunkNum))).first;

            m_order.push_front(it);
            it->second.orderIt =
                makeUnique<ClipInfo::Order::iterator>(m_order.begin());

            if (depth < m_fastCache.size()) m_fastCache[depth] = it;

            return true;
        }
    }

    void clip();
    void clip(const Id& chunkId);
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


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

namespace entwine
{

class Clipper
{
private:
    struct ClipInfo
    {
        ClipInfo() : chunkNum(0), fresh(true) { }

        explicit ClipInfo(std::size_t chunkNum)
            : chunkNum(chunkNum)
            , fresh(true)
        { }

        std::size_t chunkNum;
        bool fresh;
    };

public:
    Clipper(Builder& builder, Origin origin)
        : m_builder(builder)
        , m_startDepth(builder.metadata().structure().coldDepthBegin())
        , m_id(origin)
        , m_clips()
        , m_fastCache(32, m_clips.end())
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

            if (it != m_clips.end())
            {
                if (it->first == chunkId)
                {
                    it->second.fresh = true;
                    return false;
                }
            }
        }

        auto find(m_clips.find(chunkId));

        if (find != m_clips.end())
        {
            if (depth < m_fastCache.size()) m_fastCache[depth] = find;

            find->second.fresh = true;
            return false;
        }
        else
        {
            auto it(m_clips.insert(
                        std::make_pair(chunkId, ClipInfo(chunkNum))).first);

            if (depth < m_fastCache.size()) m_fastCache[depth] = it;

            return true;
        }
    }

    void clip();
    std::size_t id() const { return m_id; }
    std::size_t size() const { return m_clips.size(); }

private:
    typedef std::list<const Id*> Order;

    Builder& m_builder;
    const std::size_t m_startDepth;
    const uint64_t m_id;

    using ClipMap = std::map<Id, ClipInfo>;

    ClipMap m_clips;
    std::vector<ClipMap::iterator> m_fastCache;
};

} // namespace entwine


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
#include <unordered_set>

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
        , m_id(origin)
        , m_clips()
    { }

    ~Clipper()
    {
        for (const auto& c : m_clips)
        {
            m_builder.clip(c.first, c.second.chunkNum, m_id);
        }
    }

    bool insert(const Id& chunkId, std::size_t chunkNum)
    {
        const auto find(m_clips.find(chunkId));

        if (find != m_clips.end())
        {
            find->second.fresh = true;
            return false;
        }
        else
        {
            m_clips.insert(std::make_pair(chunkId, ClipInfo(chunkNum)));
            return true;
        }
    }

    void clip();
    std::size_t id() const { return m_id; }
    std::size_t size() const { return m_clips.size(); }

private:
    typedef std::list<const Id*> Order;

    Builder& m_builder;
    uint64_t m_id;

    std::unordered_map<Id, ClipInfo> m_clips;
};

} // namespace entwine


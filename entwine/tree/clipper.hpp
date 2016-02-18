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

#include <entwine/tree/builder.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class Clipper
{
public:
    Clipper(Builder& builder, Origin origin)
        : m_builder(builder)
        , m_clips()
        , m_order()
        , m_id(origin)
    { }

    ~Clipper()
    {
        for (const auto& c : m_clips)
        {
            m_builder.clip(c.first, c.second.chunkNum, m_id);
        }
    }

    bool insert(const Id& chunkId, std::size_t chunkNum);
    void clip(float ratio);
    std::size_t id() const { return m_id; }
    std::size_t size() const { return m_clips.size(); }

private:
    typedef std::list<const Id*> Order;

    struct ClipInfo
    {
        ClipInfo() : chunkNum(0), it() { }
        explicit ClipInfo(std::size_t chunkNum) : chunkNum(chunkNum), it() { }

        std::size_t chunkNum;
        Order::iterator it;
    };

    Builder& m_builder;

    std::unordered_map<Id, ClipInfo> m_clips;
    std::set<Id> m_removed;
    Order m_order;

    uint64_t m_id;
};

} // namespace entwine


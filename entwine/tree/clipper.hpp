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
#include <set>
#include <unordered_set>

#include <entwine/tree/builder.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class Clipper
{
public:
    Clipper(Builder& builder)
        : m_builder(builder)
        , m_clips()
        , m_nums()
    { }

    ~Clipper()
    {
        auto cEnd(m_clips.end());

        auto cIt(m_clips.begin());
        auto nIt(m_nums.begin());

        for ( ; cIt != cEnd; ++cIt, ++nIt)
        {
            m_builder.clip(*cIt, *nIt, this);
        }
    }

    bool insert(const Id& chunkId, const std::size_t chunkNum)
    {
        m_nums.insert(chunkNum);
        return m_clips.insert(chunkId).second;
    }

private:
    Builder& m_builder;

    std::unordered_set<Id> m_clips;
    std::unordered_set<std::size_t> m_nums;
};

} // namespace entwine


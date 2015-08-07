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
    { }

    ~Clipper()
    {
        for (const auto& info : m_clips)
        {
            m_builder.clip(info.chunkId, info.chunkNum, this);
        }
    }

    bool insert(const Id& chunkId, const std::size_t chunkNum)
    {
        return m_clips.insert(IdInfo(chunkId, chunkNum)).second;
    }

private:
    Builder& m_builder;

    struct IdInfo
    {
        IdInfo(const Id& chunkId, std::size_t chunkNum)
            : chunkId(chunkId)
            , chunkNum(chunkNum)
        { }

        bool operator<(const IdInfo& rhs) const
        {
            // return chunkId < rhs.chunkId;
            return chunkNum < rhs.chunkNum;
        }

        Id chunkId;
        std::size_t chunkNum;
    };

    std::set<IdInfo> m_clips;
};

} // namespace entwine


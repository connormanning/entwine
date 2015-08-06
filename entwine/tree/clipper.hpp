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

#include <entwine/types/structure.hpp>

namespace entwine
{

class Branch;
class Builder;

class Clipper
{
public:
    Clipper(Builder& builder);
    ~Clipper();

    bool insert(const Id& chunkId, std::size_t chunkNum);

    void clip();

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
            return chunkId < rhs.chunkId;
        }

        Id chunkId;
        std::size_t chunkNum;
    };

    std::set<IdInfo> m_clips;
};

} // namespace entwine


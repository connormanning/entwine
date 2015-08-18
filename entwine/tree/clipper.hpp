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

struct Clip
{
    Clip(const Id& id, std::size_t num) : id(id), num(num) { }

    Id id;
    std::size_t num;

    bool operator<(const Clip& other) const
    {
        return num < other.num;
    }

    bool operator==(const Clip& other) const
    {
        return num == other.num;
    }
};

}

namespace std
{
    template<> struct hash<entwine::Clip>
    {
        std::size_t operator()(const entwine::Clip& c) const
        {
            return std::hash<std::size_t>()(c.num);
        }
    };
}

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
        for (auto c(m_clips.begin()); c != m_clips.end(); ++c)
        {
            m_builder.clip(c->id, c->num, this);
        }
    }

    bool insert(const Id& chunkId, const std::size_t chunkNum)
    {
        return m_clips.insert(Clip(chunkId, chunkNum)).second;
    }

private:
    Builder& m_builder;

    std::unordered_set<Clip> m_clips;
};

} // namespace entwine


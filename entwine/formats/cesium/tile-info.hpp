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
#include <map>
#include <vector>

#include <json/json.h>

#include <entwine/types/defs.hpp>
#include <entwine/types/bounds.hpp>

namespace entwine
{

namespace arbiter { class Endpoint; }

class Metadata;

namespace cesium
{

class TileInfo
{
public:
    TileInfo() : m_visited(false) { }

    TileInfo(
            const Id& id,
            std::map<std::size_t, std::size_t> ticks,
            std::size_t depth,
            const Bounds& bounds)
        : m_id(id)
        , m_ticks(ticks)
        , m_depth(depth)
        , m_bounds(bounds)
        , m_visited(false)
        , m_children()
    { }

    const Id& id() const { return m_id; }
    const std::map<std::size_t, std::size_t>& ticks() const { return m_ticks; }
    std::size_t depth() const { return m_depth; }
    const Bounds& bounds() const { return m_bounds; }

    bool visited() const { return m_visited; }
    void visit() { m_visited = true; }

    void write(
            const Metadata& metadata,
            const arbiter::Endpoint& endpoint,
            double geometricError) const;

    bool restart(
            const Metadata& metadata,
            const arbiter::Endpoint& endpoint,
            double geometricError,
            std::size_t depth,
            std::size_t tick) const;

    bool insertInto(
            Json::Value& json,
            const Metadata& metadata,
            const arbiter::Endpoint& endpoint,
            double geometricError,
            std::size_t depth,
            std::size_t tick = 0) const;

    // Returns true if this tile has been visited in our reverse traversal.
    // Otherwise, we are the first visitor so we must keep traversing upward.
    bool addChild(const TileInfo& child)
    {
        if (child.id() == m_id)
        {
            throw std::runtime_error("Can't add self - " + m_id.str());
        }

        bool done(visited());
        visit();
        m_children[child.id()] = &child;
        return done;
    }

private:
    Bounds conformingBounds(const Metadata& metadata, std::size_t tick) const;

    Id m_id;
    std::map<std::size_t, std::size_t> m_ticks;
    std::size_t m_depth;
    Bounds m_bounds;

    bool m_visited;
    std::map<Id, const TileInfo*> m_children;
};

} // namespace cesium
} // namespace entwine


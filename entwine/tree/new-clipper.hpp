/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstdint>
#include <set>

#include <entwine/types/defs.hpp>

namespace entwine
{

struct Position
{
    Position() = default;

    // TODO Remove Z default param.
    Position(uint64_t d, uint64_t x, uint64_t y, uint64_t z = 0)
        : d(d), x(x), y(y), z(z)
    { }

    uint64_t d = 0;
    uint64_t x = 0;
    uint64_t y = 0;
    uint64_t z = 0;
};

inline bool operator<(const Position& a, const Position& b)
{
    if (a.d < b.d) return true;
    if (a.d == b.d)
    {
        if (a.x < b.x) return true;
        else if (a.x == b.x)
        {
            if (a.y < b.y) return true;
            else if (a.y == b.y)
            {
                if (a.z < b.z) return true;
            }
        }
    }
    return false;
}

class Registry;

class NewClipper
{
public:
    NewClipper(Registry& registry, Origin origin)
        : m_registry(registry)
        , m_origin(origin)
    { }

    ~NewClipper()
    {
        clip();
    }

    bool insert(uint64_t d, uint64_t x, uint64_t y)
    {
        return m_clips.emplace(d, x, y).second;
    }

    void clip();

    const Origin origin() const { return m_origin; }

private:
    Registry& m_registry;
    const Origin m_origin;

    std::set<Position> m_clips;
};


} // namespace entwine


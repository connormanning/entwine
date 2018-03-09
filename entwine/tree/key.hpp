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

#include <entwine/types/bounds.hpp>
#include <entwine/types/dir.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point.hpp>

namespace entwine
{

struct Xyz
{
    Xyz() { }
    Xyz(uint64_t x, uint64_t y, uint64_t z) : x(x), y(y), z(z) { }

    void reset() { x = 0; y = 0; z = 0; }

    std::string toString() const
    {
        return
            std::to_string(x) + '-' +
            std::to_string(y) + '-' +
            std::to_string(z);
    }

    std::string toString(std::size_t d) const
    {
        return (d < 10 ? "0" : "") + std::to_string(d) + '-' + toString();
    }

    uint64_t x = 0;
    uint64_t y = 0;
    uint64_t z = 0;
};

inline bool operator<(const Xyz& a, const Xyz& b)
{
    return (a.x < b.x) ||
        (a.x == b.x && a.y < b.y) ||
        (a.x == b.x && a.y == b.y && a.z < b.z);
}

struct Key
{
    Key(const Metadata& metadata) : m(metadata) { }

    void reset()
    {
        b = m.boundsScaledCubic();
        p.reset();
    }

    void step(const Point& g)
    {
        const auto dir(getDirection(b.mid(), g));

        p.x = (p.x << 1) | (isEast(dir)  ? 1u : 0u);
        p.y = (p.y << 1) | (isNorth(dir) ? 1u : 0u);
        p.z = (p.z << 1) | (isUp(dir)    ? 1u : 0u);

        b.go(dir);
    }

    const Bounds& bounds() const { return b; }
    const Xyz& position() const { return p; }

    const Metadata& m;

    Bounds b;
    Xyz p;
};

} // namespace entwine


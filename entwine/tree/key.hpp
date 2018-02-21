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

#include <entwine/types/bounds.hpp>
#include <entwine/types/dir.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point.hpp>

namespace entwine
{

struct Key
{
    Key(const Metadata& metadata) : m(metadata) { }

    void reset()
    {
        b = m.boundsScaledCubic();
        x = 0;
        y = 0;
        z = 0;
    }

    void step(const Point& p)
    {
        const auto dir(getDirection(b.mid(), p));

        x = (x << 1) | (isEast(dir)  ? 1u : 0u);
        y = (y << 1) | (isNorth(dir) ? 1u : 0u);
        z = (z << 1) | (isUp(dir)    ? 1u : 0u);

        b.go(dir);
    }

    std::string toString() const
    {
        return
            std::to_string(x) + '/' +
            std::to_string(y) + '/' +
            std::to_string(z);
    }

    const Bounds& bounds() const { return b; }

    const Metadata& m;

    Bounds b;
    uint64_t x = 0;
    uint64_t y = 0;
    uint64_t z = 0;
};

} // namespace entwine


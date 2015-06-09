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

#include <cmath>

namespace entwine
{

class Schema;

class Point
{
public:
    Point() noexcept
        : x(Point::emptyCoord())
        , y(Point::emptyCoord())
        , z(Point::emptyCoord())
    { }

    Point(double x, double y, double z) noexcept : x(x), y(y), z(z) { }

    // Calculates the distance-squared to another point.
    double sqDist(const Point& other) const
    {
        const double xDelta(x - other.x);
        const double yDelta(y - other.y);
        const double zDelta(z - other.z);

        return xDelta * xDelta + yDelta * yDelta + zDelta * zDelta;
    }

    static bool exists(Point p)
    {
        return
            p.x != Point::emptyCoord() ||
            p.y != Point::emptyCoord() ||
            p.z != Point::emptyCoord();
    }

    static bool exists(double x, double y, double z)
    {
        return exists(Point(x, y, z));
    }

    static double emptyCoord()
    {
        return 0;
    }

    double x;
    double y;
    double z;
};

} // namespace entwine


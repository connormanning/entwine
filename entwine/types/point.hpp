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
    Point() noexcept : x(Point::emptyCoord()), y(Point::emptyCoord()) { }
    Point(double x, double y) : x(x), y(y) { }

    // Calculates the distance-squared to another point.
    double sqDist(const Point& other) const
    {
        return (x - other.x) * (x - other.x) + (y - other.y) * (y - other.y);
    }

    static bool exists(Point p)
    {
        return p.x != Point::emptyCoord() || p.y != Point::emptyCoord();
    }

    static bool exists(double x, double y)
    {
        return exists(Point(x, y));
    }

    static double emptyCoord()
    {
        return 0;
    }

    double x;
    double y;
};

} // namespace entwine


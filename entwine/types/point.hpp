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
    Point() : x(0), y(0) { }
    Point(double x, double y) : x(x), y(y) { }
    Point(const Point& other) : x(other.x), y(other.y) { }

    // Calculates the distance-squared to another point.
    double sqDist(const Point& other) const
    {
        return (x - other.x) * (x - other.x) + (y - other.y) * (y - other.y);
    }

    static bool exists(Point p)
    {
        return
            (p.x != INFINITY && p.y != INFINITY) &&
            (p.x != 0 && p.y != 0);
    }

    static bool exists(double x, double y)
    {
        return exists(Point(x, y));
    }

    double x;
    double y;
};

} // namespace entwine


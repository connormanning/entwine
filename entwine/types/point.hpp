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
#include <iomanip>
#include <limits>
#include <ostream>
#include <vector>

namespace entwine
{

using Transformation = std::vector<double>;

class Schema;

class Point
{
public:
    Point() noexcept
        : x(Point::emptyCoord())
        , y(Point::emptyCoord())
        , z(Point::emptyCoord())
    { }

    Point(double x, double y) noexcept : x(x), y(y), z(Point::emptyCoord()) { }
    Point(double x, double y, double z) noexcept : x(x), y(y), z(z) { }

    double sqDist2d(const Point& other) const
    {
        const double xDelta(x - other.x);
        const double yDelta(y - other.y);

        return xDelta * xDelta + yDelta * yDelta;
    }

    // Calculates the distance-squared to another point.
    double sqDist3d(const Point& other) const
    {
        const double zDelta(z - other.z);
        return sqDist2d(other) + zDelta * zDelta;
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

    static Point max(const Point& a, const Point& b)
    {
        return Point(
                std::max(a.x, b.x),
                std::max(a.y, b.y),
                std::max(a.z, b.z));
    }

    static Point min(const Point& a, const Point& b)
    {
        return Point(
                std::min(a.x, b.x),
                std::min(a.y, b.y),
                std::min(a.z, b.z));
    }

    static Point normalize(const Point& p)
    {
        const double m(std::sqrt(p.x * p.x + p.y * p.y + p.z * p.z));
        return Point(p.x / m, p.y / m, p.z / m);
    }

    static Point cross(const Point& a, const Point& b)
    {
        return Point(
                a.y * b.z - a.z * b.y,
                a.z * b.x - a.x * b.z,
                a.x * b.y - a.y * b.x);
    }

    static double dot(const Point& a, const Point& b)
    {
        return a.x * b.x + a.y * b.y + a.z * b.z;
    }

    static Point transform(const Point& p, const Transformation& t)
    {
        return Point(
            p.x * t[0] + p.y * t[1] + p.z * t[2] + t[3],
            p.x * t[4] + p.y * t[5] + p.z * t[6] + t[7],
            p.z != emptyCoord() ?
                p.x * t[8] + p.y * t[9] + p.z * t[10] + t[11] :
                0);
    }

    double x;
    double y;
    double z;
};

inline bool ltChained(const Point& lhs, const Point& rhs)
{
    return
        (lhs.x < rhs.x) ||
        (lhs.x == rhs.x &&
            (lhs.y < rhs.y ||
            (lhs.y == rhs.y &&
                (lhs.z < rhs.z))));
}

inline bool operator<(const Point& lhs, const Point& rhs)
{
    return lhs.x < rhs.x && lhs.y < rhs.y && lhs.z < rhs.z;
}

inline bool operator<=(const Point& lhs, const Point& rhs)
{
    return lhs.x <= rhs.x && lhs.y <= rhs.y && lhs.z <= rhs.z;
}

inline bool operator>(const Point& lhs, const Point& rhs)
{
    return lhs.x > rhs.x && lhs.y > rhs.y && lhs.z > rhs.z;
}

inline bool operator>=(const Point& lhs, const Point& rhs)
{
    return lhs.x >= rhs.x && lhs.y >= rhs.y && lhs.z >= rhs.z;
}

inline bool operator==(const Point& lhs, const Point& rhs)
{
    return lhs.x == rhs.x && lhs.y == rhs.y && lhs.z == rhs.z;
}

inline bool operator!=(const Point& lhs, const Point& rhs)
{
    return !(lhs == rhs);
}

inline Point operator+(const Point& in, double offset)
{
    return Point(in.x + offset, in.y + offset, in.z + offset);
}

inline Point operator-(const Point& in, double offset)
{
    return in + (-offset);
}

inline Point operator+(const Point& lhs, const Point& rhs)
{
    return Point(lhs.x + rhs.x, lhs.y + rhs.y, lhs.z + rhs.z);
}

inline Point operator-(const Point& lhs, const Point& rhs)
{
    return Point(lhs.x - rhs.x, lhs.y - rhs.y, lhs.z - rhs.z);
}

inline Point& operator+=(Point& lhs, const Point& rhs)
{
    lhs.x += rhs.x;
    lhs.y += rhs.y;
    lhs.z += rhs.z;
    return lhs;
}

inline Point& operator-=(Point& lhs, const Point& rhs)
{
    lhs.x -= rhs.x;
    lhs.y -= rhs.y;
    lhs.z -= rhs.z;
    return lhs;
}

inline Point operator*(const Point& p, double s)
{
    return Point(p.x * s, p.y * s, p.z * s);
}

inline std::ostream& operator<<(std::ostream& os, const Point& point)
{
    auto flags(os.flags());
    auto precision(os.precision());

    os << std::setprecision(5) << std::fixed;

    os << "(" << point.x << ", " << point.y;
    if (
            point.z != Point::emptyCoord() &&
            point.z != std::numeric_limits<double>::max() &&
            point.z != std::numeric_limits<double>::lowest())
    {
        os << ", " << point.z;
    }
    os << ")";

    os << std::setprecision(precision);
    os.flags(flags);

    return os;
}

class Color
{
public:
    Color() : r(0), g(0), b(0) { }
    Color(uint8_t r, uint8_t g, uint8_t b) : r(r), g(g), b(b) { }

    static Color min(const Color& a, const Color& b)
    {
        return Color(
                std::min(a.r, b.r),
                std::min(a.g, b.g),
                std::min(a.b, b.b));
    }

    static Color max(const Color& a, const Color& b)
    {
        return Color(
                std::max(a.r, b.r),
                std::max(a.g, b.g),
                std::max(a.b, b.b));
    }

    uint8_t r, g, b;
};

inline std::ostream& operator<<(std::ostream& os, const Color& c)
{
    os << "(" << (int)c.r << ", " << (int)c.g << ", " << (int)c.b << ")";
    return os;
}

} // namespace entwine


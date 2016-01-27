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

#include <entwine/third/json/json.hpp>
#include <entwine/types/point.hpp>

namespace entwine
{

class Range;

class BBox
{
public:
    BBox();
    BBox(Point min, Point max, bool is3d);
    BBox(const BBox& other);
    BBox(const Json::Value& json);

    void set(Point min, Point max, bool is3d);

    const Point& min() const { return m_min; }
    const Point& max() const { return m_max; }
    const Point& mid() const { return m_mid; }

    // Returns true if this BBox shares any area in common with another.
    bool overlaps(const BBox& other) const;

    // Returns true if the requested point is contained within this BBox.
    bool contains(const Point& p) const;

    double width() const;   // Length in X.
    double depth() const;   // Length in Y.
    double height() const;  // Length in Z.

    void goNwu();
    void goNwd(bool force2d = false);
    void goNeu();
    void goNed(bool force2d = false);
    void goSwu();
    void goSwd(bool force2d = false);
    void goSeu();
    void goSed(bool force2d = false);

    bool exists() const { return Point::exists(m_min) && Point::exists(m_max); }
    bool is3d() const { return m_is3d; }

    Json::Value toJson() const;

    void grow(const BBox& bbox);
    void grow(const Point& p);
    void growZ(const Range& range);

    // Bloat coordinates to nearest integer.
    void bloat()
    {
        m_min.x = std::floor(m_min.x);
        m_min.y = std::floor(m_min.y);
        m_min.z = std::floor(m_min.z);
        m_max.x = std::ceil(m_max.x);
        m_max.y = std::ceil(m_max.y);
        m_max.z = std::ceil(m_max.z);

        setMid();
    }

    // Bloat all coordinates necessary to form a cube.
    void cubeify()
    {
        const double xDist(m_max.x - m_min.x);
        const double yDist(m_max.y - m_min.y);
        const double zDist(m_max.z - m_min.z);

        const double radius(std::max(std::max(xDist, yDist), zDist) / 2 + 10);

        m_min.x = std::floor(m_mid.x) - radius;
        m_min.y = std::floor(m_mid.y) - radius;
        m_min.z = std::floor(m_mid.z) - radius;

        m_max.x = std::floor(m_mid.x) + radius;
        m_max.y = std::floor(m_mid.y) + radius;
        m_max.z = std::floor(m_mid.z) + radius;
    }

private:
    Point m_min;
    Point m_max;
    Point m_mid;

    bool m_is3d;

    void setMid();

    void check(const Point& min, const Point& max) const;
};

std::ostream& operator<<(std::ostream& os, const BBox& bbox);

inline bool operator==(const BBox& lhs, const BBox& rhs)
{
    return lhs.min() == rhs.min() && lhs.max() == rhs.max();
}

} // namespace entwine


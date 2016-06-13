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
#include <entwine/types/dir.hpp>
#include <entwine/types/point.hpp>

namespace entwine
{

class Range;

class BBox
{
public:
    BBox();
    BBox(const Point& min, const Point& max);
    BBox(const Point& min, const Point& max, bool is3d);
    BBox(const BBox& other);
    BBox(const Json::Value& json);

    void set(const BBox& other);
    void set(const Point& min, const Point& max, bool is3d);

    const Point& min() const { return m_min; }
    const Point& max() const { return m_max; }
    const Point& mid() const { return m_mid; }

    // Returns true if this BBox shares any area in common with another.
    bool overlaps(const BBox& other, bool force2d = false) const;

    // Returns true if the requested BBox is contained within this BBox.
    bool contains(const BBox& other, bool force2d = false) const;

    // Returns true if the requested point is contained within this BBox.
    bool contains(const Point& p) const;

    double width()  const { return m_max.x - m_min.x; } // Length in X.
    double depth()  const { return m_max.y - m_min.y; } // Length in Y.
    double height() const { return m_max.z - m_min.z; } // Length in Z.

    double area() const { return width() * depth(); }
    double volume() const { return width() * depth() * height(); }

    void goNwu()
    {
        m_max.x = m_mid.x;
        m_min.y = m_mid.y;
        if (m_is3d) m_min.z = m_mid.z;
        setMid();
    }

    void goNwd(const bool force2d = false)
    {
        m_max.x = m_mid.x;
        m_min.y = m_mid.y;
        if (!force2d && m_is3d) m_max.z = m_mid.z;
        setMid();
    }

    void goNeu()
    {
        m_min.x = m_mid.x;
        m_min.y = m_mid.y;
        if (m_is3d) m_min.z = m_mid.z;
        setMid();
    }

    void goNed(const bool force2d = false)
    {
        m_min.x = m_mid.x;
        m_min.y = m_mid.y;
        if (!force2d && m_is3d) m_max.z = m_mid.z;
        setMid();
    }

    void goSwu()
    {
        m_max.x = m_mid.x;
        m_max.y = m_mid.y;
        if (m_is3d) m_min.z = m_mid.z;
        setMid();
    }

    void goSwd(const bool force2d = false)
    {
        m_max.x = m_mid.x;
        m_max.y = m_mid.y;
        if (!force2d && m_is3d) m_max.z = m_mid.z;
        setMid();
    }

    void goSeu()
    {
        m_min.x = m_mid.x;
        m_max.y = m_mid.y;
        if (m_is3d) m_min.z = m_mid.z;
        setMid();
    }

    void goSed(const bool force2d = false)
    {
        m_min.x = m_mid.x;
        m_max.y = m_mid.y;
        if (!force2d && m_is3d) m_max.z = m_mid.z;
        setMid();
    }

    BBox getNwd(bool force2d = false) const
    {
        BBox b(*this); b.goNwd(force2d); return b;
    }

    BBox getNed(bool force2d = false) const
    {
        BBox b(*this); b.goNed(force2d); return b;
    }

    BBox getSwd(bool force2d = false) const
    {
        BBox b(*this); b.goSwd(force2d); return b;
    }

    BBox getSed(bool force2d = false) const
    {
        BBox b(*this); b.goSed(force2d); return b;
    }

    BBox getNwu() const { BBox b(*this); b.goNwu(); return b; }
    BBox getNeu() const { BBox b(*this); b.goNeu(); return b; }
    BBox getSwu() const { BBox b(*this); b.goSwu(); return b; }
    BBox getSeu() const { BBox b(*this); b.goSeu(); return b; }

    void go(Dir dir, bool force2d = false)
    {
        if (force2d) dir = toDir(toIntegral(dir, true));

        switch (dir)
        {
            case Dir::swd: goSwd(force2d); break;
            case Dir::sed: goSed(force2d); break;
            case Dir::nwd: goNwd(force2d); break;
            case Dir::ned: goNed(force2d); break;
            case Dir::swu: goSwu(); break;
            case Dir::seu: goSeu(); break;
            case Dir::nwu: goNwu(); break;
            case Dir::neu: goNeu(); break;
        }
    }

    BBox get(Dir dir) const
    {
        switch (dir)
        {
            case Dir::swd: return getSwd(); break;
            case Dir::sed: return getSed(); break;
            case Dir::nwd: return getNwd(); break;
            case Dir::ned: return getNed(); break;
            case Dir::swu: return getSwu(); break;
            case Dir::seu: return getSeu(); break;
            case Dir::nwu: return getNwu(); break;
            case Dir::neu: return getNeu(); break;
        }

        throw std::runtime_error(
                "Invalid Dir to BBox::get: " + std::to_string(toIntegral(dir)));
    }

    bool exists() const { return Point::exists(m_min) && Point::exists(m_max); }
    bool is3d() const { return m_is3d; }

    Json::Value toJson() const;

    void grow(const BBox& bbox);
    void grow(const Point& p);
    void growZ(const Range& range);

    bool isCubic() const
    {
        return width() == depth() && (!m_is3d || width() == height());
    }

    // Bloat all coordinates necessary to form a cube and also to the nearest
    // integer.
    BBox cubeify() const
    {
        const double xDist(m_max.x - m_min.x);
        const double yDist(m_max.y - m_min.y);
        const double zDist(m_max.z - m_min.z);

        const double radius(
                std::ceil(std::max(std::max(xDist, yDist), zDist) / 2.0 + 10));

        return BBox(
                Point(
                    std::floor(m_mid.x - radius),
                    std::floor(m_mid.y - radius),
                    std::floor(m_mid.z - radius)),
                Point(
                    std::floor(m_mid.x + radius),
                    std::floor(m_mid.y + radius),
                    std::floor(m_mid.z + radius)));
    }

    BBox growBy(double ratio) const;

    std::vector<BBox> explode() const;
    std::vector<BBox> explode(std::size_t delta) const;

private:
    Point m_min;
    Point m_max;
    Point m_mid;

    bool m_is3d;

    void setMid();

    void check(const Point& min, const Point& max) const;
};

std::ostream& operator<<(std::ostream& os, const BBox& bbox);

// Orders BBoxes by their midpoint.  This is really only useful if the boxes
// are arranged in a grid and are of equal size (like during a MetaQuery).
inline bool operator<(const BBox& lhs, const BBox& rhs)
{
    const auto& lhsMid(lhs.mid());
    const auto& rhsMid(rhs.mid());

    return
        (lhsMid.x < rhsMid.x) ||
        (lhsMid.x == rhsMid.x && lhsMid.y < rhsMid.y) ||
        (lhsMid.x == rhsMid.x && lhsMid.y == rhsMid.y && lhsMid.z < rhsMid.z);
}

inline bool operator==(const BBox& lhs, const BBox& rhs)
{
    return lhs.min() == rhs.min() && lhs.max() == rhs.max();
}

inline bool operator!=(const BBox& lhs, const BBox& rhs)
{
    return !(lhs == rhs);
}

} // namespace entwine


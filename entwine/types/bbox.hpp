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

#include <entwine/third/json/json.h>
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

    const Point& min() const;
    const Point& max() const;
    const Point& mid() const;

    // Returns true if this BBox shares any area in common with another.
    bool overlaps(const BBox& other) const;

    // Returns true if the requested point is contained within this BBox.
    bool contains(const Point& p) const;

    double width() const;   // Length in X.
    double depth() const;   // Length in Y.
    double height() const;  // Length in Z.

    void goNw();
    void goNe();
    void goSw();
    void goSe();

    void goNwu();
    void goNwd();
    void goNeu();
    void goNed();
    void goSwu();
    void goSwd();
    void goSeu();
    void goSed();

    bool exists() const;

    Json::Value toJson() const;

    void grow(const Point& p);
    void growZ(const Range& range);

private:
    Point m_min;
    Point m_max;
    Point m_mid;

    bool m_is3d;

    void setMid();

    void check(const Point& min, const Point& max) const;
};

} // namespace entwine


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

#include <json/json.h>

#include "point.hpp"

namespace entwine
{

class BBox
{
public:
    BBox();
    BBox(Point min, Point max);
    BBox(const BBox& other);

    Point min() const;
    Point max() const;
    Point mid() const;

    void set(const Point& min, const Point& max);

    // Returns true if this BBox shares any area in common with another.
    bool overlaps(const BBox& other) const;

    // Returns true if the requested point is contained within this BBox.
    bool contains(const Point& p) const;

    double width() const;
    double height() const;

    BBox getNe() const;
    BBox getNw() const;
    BBox getSw() const;
    BBox getSe() const;

    // Add an epsilon to the maxes.  Useful when bounds are set by specifying
    // the min/max bounds encountered in a specific file - when a bbox is
    // created with those bounds, the points that constituted the max bounds
    // would not be included because of the [min, max) convention used here.
    BBox encapsulate() const;

    Json::Value toJson() const;
    static BBox fromJson(const Json::Value& json);

private:
    Point m_min;
    Point m_max;

    void check(const Point& min, const Point& max) const;
};

} // namespace entwine


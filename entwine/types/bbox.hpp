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

class BBox
{
public:
    BBox();
    BBox(Point min, Point max);
    BBox(const BBox& other);

    void set(Point min, Point max);

    Point min() const;
    Point max() const;
    Point mid() const;

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

    bool exists() const;

    Json::Value toJson() const;
    static BBox fromJson(const Json::Value& json);

    void grow(const Point& p);

private:
    Point m_min;
    Point m_max;

    void check(const Point& min, const Point& max) const;
};

} // namespace entwine


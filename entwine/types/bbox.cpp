/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/bbox.hpp>

#include <cmath>
#include <limits>
#include <iostream>

namespace entwine
{

BBox::BBox() : m_min(), m_max() { }

BBox::BBox(Point min, Point max)
    : m_min(std::min(min.x, max.x), std::min(min.y, max.y))
    , m_max(std::max(min.x, max.x), std::max(min.y, max.y))
{
    check(min, max);
}

BBox::BBox(const BBox& other)
    : m_min(other.min())
    , m_max(other.max())
{ }

Point BBox::min() const { return m_min; }
Point BBox::max() const { return m_max; }
Point BBox::mid() const
{
    return Point(
            m_min.x + (m_max.x - m_min.x) / 2.0,
            m_min.y + (m_max.y - m_min.y) / 2.0);
}

bool BBox::overlaps(const BBox& other) const
{
    Point middle(mid());
    Point otherMiddle(other.mid());

    return
        std::abs(middle.x - otherMiddle.x) <
            width() / 2.0  + other.width() / 2.0 &&
        std::abs(middle.y - otherMiddle.y) <
            height() / 2.0 + other.height() / 2.0;
}

bool BBox::contains(const Point& p) const
{
    return p.x >= m_min.x && p.y >= m_min.y && p.x < m_max.x && p.y < m_max.y;
}

double BBox::width()  const { return m_max.x - m_min.x; }
double BBox::height() const { return m_max.y - m_min.y; }

BBox BBox::getNw() const
{
    const Point middle(mid());
    return BBox(Point(m_min.x, middle.y), Point(middle.x, m_max.y));
}

BBox BBox::getNe() const
{
    const Point middle(mid());
    return BBox(Point(middle.x, middle.y), Point(m_max.x, m_max.y));
}

BBox BBox::getSw() const
{
    const Point middle(mid());
    return BBox(Point(m_min.x, m_min.y), Point(middle.x, middle.y));
}

BBox BBox::getSe() const
{
    const Point middle(mid());
    return BBox(Point(middle.x, m_min.y), Point(m_max.x, middle.y));
}

BBox BBox::encapsulate() const
{
    return BBox(
            m_min,
            Point(
                m_max.x + std::numeric_limits<double>::min(),
                m_max.y + std::numeric_limits<double>::min()));
}

bool BBox::exists() const
{
    return Point::exists(m_min) && Point::exists(m_max);
}

Json::Value BBox::toJson() const
{
    Json::Value json;
    json.append(m_min.x);
    json.append(m_min.y);
    json.append(m_max.x);
    json.append(m_max.y);
    return json;
}

BBox BBox::fromJson(const Json::Value& json)
{
    return BBox(
            Point(
                json.get(Json::ArrayIndex(0), 0).asDouble(),
                json.get(Json::ArrayIndex(1), 0).asDouble()),
            Point(
                json.get(Json::ArrayIndex(2), 0).asDouble(),
                json.get(Json::ArrayIndex(3), 0).asDouble()));
}

void BBox::check(const Point& min, const Point& max) const
{
    if (min.x > max.x || min.y > max.y)
    {
        std::cout << "Correcting malformed BBox" << std::endl;
    }
}

} // namespace entwine


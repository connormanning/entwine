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

#include <entwine/types/range.hpp>

namespace entwine
{

BBox::BBox() : m_min(), m_max() { }

BBox::BBox(const Point min, const Point max)
    : m_min(
            std::min(min.x, max.x),
            std::min(min.y, max.y),
            std::min(min.z, max.z))
    , m_max(
            std::max(min.x, max.x),
            std::max(min.y, max.y),
            std::max(min.z, max.z))
    , m_mid()
{
    setMid();
    check(min, max);
}

BBox::BBox(const BBox& other)
    : m_min(other.min())
    , m_max(other.max())
    , m_mid()
{
    setMid();
}

BBox::BBox(const Json::Value& json)
    : m_min(
            Point(
                json.get(Json::ArrayIndex(0), 0).asDouble(),
                json.get(Json::ArrayIndex(1), 0).asDouble(),
                json.get(Json::ArrayIndex(2), 0).asDouble()))
    , m_max(
            Point(
                json.get(Json::ArrayIndex(3), 0).asDouble(),
                json.get(Json::ArrayIndex(4), 0).asDouble(),
                json.get(Json::ArrayIndex(5), 0).asDouble()))
    , m_mid()
{
    setMid();
}

void BBox::set(const Point min, const Point max)
{
    m_min = min;
    m_max = max;
    setMid();
}

const Point& BBox::min() const { return m_min; }
const Point& BBox::max() const { return m_max; }
const Point& BBox::mid() const { return m_mid; }

bool BBox::overlaps(const BBox& other) const
{
    Point otherMid(other.mid());

    return
        std::abs(m_mid.x - otherMid.x) <=
            width() / 2.0  + other.width() / 2.0 &&
        std::abs(m_mid.y - otherMid.y) <=
            depth() / 2.0 + other.depth() / 2.0 &&
        std::abs(m_mid.z - otherMid.z) <=
            height() / 2.0 + other.height() / 2.0;

}

bool BBox::contains(const Point& p) const
{
    return p.x >= m_min.x && p.y >= m_min.y && p.x < m_max.x && p.y < m_max.y;
}

double BBox::width()    const { return m_max.x - m_min.x; }
double BBox::depth()    const { return m_max.y - m_min.y; }
double BBox::height()   const { return m_max.z - m_min.z; }

void BBox::goNw()
{
    m_max.x = m_mid.x;
    m_min.y = m_mid.y;
    setMid();
}

void BBox::goNe()
{
    m_min.x = m_mid.x;
    m_min.y = m_mid.y;
    setMid();
}

void BBox::goSw()
{
    m_max.x = m_mid.x;
    m_max.y = m_mid.y;
    setMid();
}

void BBox::goSe()
{
    m_min.x = m_mid.x;
    m_max.y = m_mid.y;
    setMid();
}




void BBox::goNwu()
{
    m_max.x = m_mid.x;
    m_min.y = m_mid.y;
    m_min.z = m_mid.z;
    setMid();
}

void BBox::goNwd()
{
    m_max.x = m_mid.x;
    m_min.y = m_mid.y;
    m_max.z = m_mid.z;
    setMid();
}

void BBox::goNeu()
{
    m_min.x = m_mid.x;
    m_min.y = m_mid.y;
    m_min.z = m_mid.z;
    setMid();
}

void BBox::goNed()
{
    m_min.x = m_mid.x;
    m_min.y = m_mid.y;
    m_max.z = m_mid.z;
    setMid();
}

void BBox::goSwu()
{
    m_max.x = m_mid.x;
    m_max.y = m_mid.y;
    m_min.z = m_mid.z;
    setMid();
}

void BBox::goSwd()
{
    m_max.x = m_mid.x;
    m_max.y = m_mid.y;
    m_max.z = m_mid.z;
    setMid();
}

void BBox::goSeu()
{
    m_min.x = m_mid.x;
    m_max.y = m_mid.y;
    m_min.z = m_mid.z;
    setMid();
}

void BBox::goSed()
{
    m_min.x = m_mid.x;
    m_max.y = m_mid.y;
    m_max.z = m_mid.z;
    setMid();
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
    json.append(m_min.z);
    json.append(m_max.x);
    json.append(m_max.y);
    json.append(m_max.z);
    return json;
}

void BBox::check(const Point& min, const Point& max) const
{
    if (min.x > max.x || min.y > max.y || min.z > max.z)
    {
        std::cout << "Correcting malformed BBox" << std::endl;
    }
}

void BBox::setMid()
{
    m_mid.x = m_min.x + (m_max.x - m_min.x) / 2.0;
    m_mid.y = m_min.y + (m_max.y - m_min.y) / 2.0;
    m_mid.z = m_min.z + (m_max.z - m_min.z) / 2.0;
}

void BBox::grow(const Point& p)
{
    m_min.x = std::min(m_min.x, p.x);
    m_min.y = std::min(m_min.y, p.y);
    m_min.z = std::min(m_min.z, p.z);
    m_max.x = std::max(m_max.x, p.x);
    m_max.y = std::max(m_max.y, p.y);
    m_max.z = std::max(m_max.z, p.z);
    setMid();
}

void BBox::growZ(const Range& range)
{
    m_min.z = std::min(m_min.z, range.min);
    m_max.z = std::max(m_max.z, range.max);
    m_mid.z = m_min.z + (m_max.z - m_min.z) / 2.0;
}

} // namespace entwine


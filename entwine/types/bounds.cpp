/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/bounds.hpp>

#include <cmath>
#include <limits>
#include <numeric>
#include <iostream>

#include <entwine/util/unique.hpp>

namespace entwine
{

Bounds::Bounds(const Point& min, const Point& max)
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
    if (min.x > max.x || min.y > max.y || min.z > max.z)
    {
        std::cout << "Correcting malformed Bounds" << std::endl;
    }
}

Bounds::Bounds(const json& j)
{
    if (j.is_null()) return;

    if (!j.is_array() || (j.size() != 4 && j.size() != 6))
    {
        throw std::runtime_error("Invalid JSON Bounds: " + j.dump(2));
    }

    if (j.size() == 6)
    {
        *this = Bounds(
                j.at(0).get<double>(),
                j.at(1).get<double>(),
                j.at(2).get<double>(),
                j.at(3).get<double>(),
                j.at(4).get<double>(),
                j.at(5).get<double>());
    }
    else
    {
        *this = Bounds(
                j.at(0).get<double>(),
                j.at(1).get<double>(),
                j.at(2).get<double>(),
                j.at(3).get<double>());
    }
}

Bounds::Bounds(const Point& center, const double radius)
    : Bounds(center - radius, center + radius)
{ }

Bounds::Bounds(
        const double xMin,
        const double yMin,
        const double zMin,
        const double xMax,
        const double yMax,
        const double zMax)
    : Bounds(Point(xMin, yMin, zMin), Point(xMax, yMax, zMax))
{ }

Bounds::Bounds(
        const double xMin,
        const double yMin,
        const double xMax,
        const double yMax)
    : Bounds(Point(xMin, yMin), Point(xMax, yMax))
{ }

void Bounds::grow(const Bounds& other)
{
    grow(other.min());
    grow(other.max());
}

void Bounds::grow(const Point& p)
{
    m_min.x = std::min(m_min.x, p.x);
    m_min.y = std::min(m_min.y, p.y);
    m_min.z = std::min(m_min.z, p.z);
    m_max.x = std::max(m_max.x, p.x);
    m_max.y = std::max(m_max.y, p.y);
    m_max.z = std::max(m_max.z, p.z);
    setMid();
}

void Bounds::shrink(const Bounds& other)
{
    m_min = Point::max(m_min, other.min());
    m_max = Point::min(m_max, other.max());
    setMid();
}

Bounds Bounds::growBy(double ratio) const
{
    const Point delta(
            (m_max.x - m_mid.x) * ratio,
            (m_max.y - m_mid.y) * ratio,
            (m_max.z - m_mid.z) * ratio);

    return Bounds(m_min - delta, m_max + delta);
}

Bounds Bounds::getNwd(bool force2d) const
{
    Bounds b(*this); b.goNwd(force2d); return b;
}

Bounds Bounds::getNed(bool force2d) const
{
    Bounds b(*this); b.goNed(force2d); return b;
}

Bounds Bounds::getSwd(bool force2d) const
{
    Bounds b(*this); b.goSwd(force2d); return b;
}

Bounds Bounds::getSed(bool force2d) const
{
    Bounds b(*this); b.goSed(force2d); return b;
}

Bounds Bounds::getNwu() const { Bounds b(*this); b.goNwu(); return b; }
Bounds Bounds::getNeu() const { Bounds b(*this); b.goNeu(); return b; }
Bounds Bounds::getSwu() const { Bounds b(*this); b.goSwu(); return b; }
Bounds Bounds::getSeu() const { Bounds b(*this); b.goSeu(); return b; }

Bounds Bounds::applyScaleOffset(const Scale& s, const Offset& o) const
{
    return Bounds(Point::scale(min(), s, o), Point::scale(max(), s, o));
}

std::ostream& operator<<(std::ostream& os, const Bounds& bounds)
{
    auto flags(os.flags());
    auto precision(os.precision());

    os << std::setprecision(2) << std::fixed;

    os << "[" << bounds.min() << ", " << bounds.max() << "]";

    os << std::setprecision(precision);
    os.flags(flags);

    return os;
}

} // namespace entwine


/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/roller.hpp>

#include <entwine/types/point.hpp>

namespace entwine
{

Roller::Roller(const BBox& bbox)
    : m_index(0)
    , m_bbox(bbox)
    , m_depth(0)
{ }

Roller::Roller(const Roller& other)
    : m_index(other.m_index)
    , m_bbox(other.m_bbox)
    , m_depth(other.m_depth)
{ }

void Roller::magnify(const Point* point)
{
    const Point mid(m_bbox.mid());

    if (point->x < mid.x)
        if (point->y < mid.y)
            goSw();
        else
            goNw();
    else
        if (point->y < mid.y)
            goSe();
        else
            goNe();
}

std::size_t Roller::depth() const
{
    return m_depth;
}

uint64_t Roller::index() const
{
    return m_index;
}

const BBox& Roller::bbox() const
{
    return m_bbox;
}

void Roller::goNw()
{
    step(Dir::nw);
    m_bbox = m_bbox.getNw();
}

void Roller::goNe()
{
    step(Dir::ne);
    m_bbox = m_bbox.getNe();
}

void Roller::goSw()
{
    step(Dir::sw);
    m_bbox = m_bbox.getSw();
}

void Roller::goSe()
{
    step(Dir::se);
    m_bbox = m_bbox.getSe();
}

Roller Roller::getNw() const
{
    Roller roller(*this);
    roller.goNw();
    return roller;
}

Roller Roller::getNe() const
{
    Roller roller(*this);
    roller.goNe();
    return roller;
}

Roller Roller::getSw() const
{
    Roller roller(*this);
    roller.goSw();
    return roller;
}

Roller Roller::getSe() const
{
    Roller roller(*this);
    roller.goSe();
    return roller;
}

void Roller::step(const Dir dir)
{
    // TODO Hard-coded to quad-tree.
    m_index = (m_index << 2) + 1 + dir;
    ++m_depth;
}

} // namespace entwine


/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/climber.hpp>

#include <entwine/types/point.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

Climber::Climber(const BBox& bbox, const Structure& structure)
    : m_structure(structure)
    , m_dimensions(structure.dimensions())
    , m_index(0)
    , m_depth(0)
    , m_bbox(bbox)
{ }

void Climber::magnify(const Point& point)
{
    const Point& mid(m_bbox.mid());

    // Up: +4, Down: +0.
    const int z(m_dimensions == 3 && point.z >= mid.z ? 4 : 0);

    // North: +0, South: +2.
    const int y(point.y >= mid.y ? 0 : 2);

    // East: +1, West: +0.
    const int x(point.x >= mid.x ? 1 : 0);

    switch (x + y + z)
    {
        case Dir::nwd: goNwd(); break;
        case Dir::ned: goNed(); break;
        case Dir::swd: goSwd(); break;
        case Dir::sed: goSed(); break;
        case Dir::nwu: goNwu(); break;
        case Dir::neu: goNeu(); break;
        case Dir::swu: goSwu(); break;
        case Dir::seu: goSeu(); break;
    }
}

std::size_t Climber::index() const
{
    return m_index;
}

std::size_t Climber::depth() const
{
    return m_depth;
}

const BBox& Climber::bbox() const
{
    return m_bbox;
}

void Climber::goNwd() { step(Dir::nwd); m_bbox.goNwd(); }
void Climber::goNed() { step(Dir::ned); m_bbox.goNed(); }
void Climber::goSwd() { step(Dir::swd); m_bbox.goSwd(); }
void Climber::goSed() { step(Dir::sed); m_bbox.goSed(); }
void Climber::goNwu() { step(Dir::nwu); m_bbox.goNwu(); }
void Climber::goNeu() { step(Dir::neu); m_bbox.goNeu(); }
void Climber::goSwu() { step(Dir::swu); m_bbox.goSwu(); }
void Climber::goSeu() { step(Dir::seu); m_bbox.goSeu(); }

Climber Climber::getNwd() const { Climber c(*this); c.goNwd(); return c; }
Climber Climber::getNed() const { Climber c(*this); c.goNed(); return c; }
Climber Climber::getSwd() const { Climber c(*this); c.goSwd(); return c; }
Climber Climber::getSed() const { Climber c(*this); c.goSed(); return c; }
Climber Climber::getNwu() const { Climber c(*this); c.goNwu(); return c; }
Climber Climber::getNeu() const { Climber c(*this); c.goNeu(); return c; }
Climber Climber::getSwu() const { Climber c(*this); c.goSwu(); return c; }
Climber Climber::getSeu() const { Climber c(*this); c.goSeu(); return c; }

/*
std::size_t Climber::chunkId() const
{
    return
        m_currentLevelBeginIndex +
        m_currentChunkPoints * (currentLevelOffset() / m_currentChunkPoints);
}

std::size_t Climber::chunkOffset() const
{
    return currentLevelOffset() % m_currentChunkPoints;
}

std::size_t Climber::chunkNum() const
{
    return
        m_currentLevelBeginNum + currentLevelOffset() / m_currentChunkPoints;
}

std::size_t Climber::currentLevelOffset() const
{
    return m_index - m_currentLevelBeginIndex;
}
*/

void Climber::step(const Dir dir)
{
    // TODO Might get some speed benefits from tracking this here, which would
    // avoid calculating them from scratch at each level in Structure::getInfo.
    /*
    // Strictly greater-than, the first cold level has a numBegin of zero.
    if (m_currentLevelBeginIndex > m_structure.coldIndexBegin())
    {
        m_currentLevelBeginNum +=
            m_currentLevelNumPoints / m_currentChunkPoints;
    }

    if (m_index >= m_structure.sparseIndexBegin())
    {
        m_currentChunkPoints *= m_structure.factor();
    }

    m_currentLevelBeginIndex = (m_currentLevelBeginIndex << m_dimensions) + 1;
    m_currentLevelNumPoints *= m_structure.factor();
    */

    m_index = (m_index << m_dimensions) + 1 + dir;
    ++m_depth;
}

} // namespace entwine


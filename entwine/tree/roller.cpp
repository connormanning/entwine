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
#include <entwine/types/structure.hpp>

namespace entwine
{

Roller::Roller(const BBox& bbox, const Structure& structure)
    : m_structure(structure)
    , m_dimensions(structure.dimensions())
    , m_index(0)
    , m_depth(0)
    , m_bbox(bbox)
{
    if (m_dimensions != 2)
    {
        throw std::runtime_error("Octree not yet supported");
    }
}

void Roller::magnify(const Point& point)
{
    const Point& mid(m_bbox.mid());

    if (m_dimensions == 2)
    {
        if (point.x < mid.x)
            if (point.y < mid.y)
                goSw();
            else
                goNw();
        else
            if (point.y < mid.y)
                goSe();
            else
                goNe();
    }
    else
    {
        // TODO
    }
}

std::size_t Roller::index() const
{
    return m_index;
}

std::size_t Roller::depth() const
{
    return m_depth;
}

const BBox& Roller::bbox() const
{
    return m_bbox;
}

void Roller::goNw()
{
    step(Dir::nw);
    m_bbox.goNw();
}

void Roller::goNe()
{
    step(Dir::ne);
    m_bbox.goNe();
}

void Roller::goSw()
{
    step(Dir::sw);
    m_bbox.goSw();
}

void Roller::goSe()
{
    step(Dir::se);
    m_bbox.goSe();
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

/*
std::size_t Roller::chunkId() const
{
    return
        m_currentLevelBeginIndex +
        m_currentChunkPoints * (currentLevelOffset() / m_currentChunkPoints);
}

std::size_t Roller::chunkOffset() const
{
    return currentLevelOffset() % m_currentChunkPoints;
}

std::size_t Roller::chunkNum() const
{
    return
        m_currentLevelBeginNum + currentLevelOffset() / m_currentChunkPoints;
}

std::size_t Roller::currentLevelOffset() const
{
    return m_index - m_currentLevelBeginIndex;
}
*/

void Roller::step(const Dir dir)
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


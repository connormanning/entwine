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
{
    if (m_dimensions != 2)
    {
        throw std::runtime_error("Octree not yet supported");
    }
}

void Climber::magnify(const Point& point)
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

void Climber::goNw()
{
    step(Dir::nw);
    m_bbox.goNw();
}

void Climber::goNe()
{
    step(Dir::ne);
    m_bbox.goNe();
}

void Climber::goSw()
{
    step(Dir::sw);
    m_bbox.goSw();
}

void Climber::goSe()
{
    step(Dir::se);
    m_bbox.goSe();
}

Climber Climber::getNw() const
{
    Climber climber(*this);
    climber.goNw();
    return climber;
}

Climber Climber::getNe() const
{
    Climber climber(*this);
    climber.goNe();
    return climber;
}

Climber Climber::getSw() const
{
    Climber climber(*this);
    climber.goSw();
    return climber;
}

Climber Climber::getSe() const
{
    Climber climber(*this);
    climber.goSe();
    return climber;
}

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


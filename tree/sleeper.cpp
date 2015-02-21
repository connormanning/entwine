/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "sleeper.hpp"

#include "tree/roller.hpp"

Sleeper::Sleeper(const BBox& bbox, const std::size_t pointSize)
    : m_bbox(bbox)
    , m_registry(pointSize)
{ }

Sleeper::Sleeper(
        const BBox& bbox,
        const std::size_t pointSize,
        std::shared_ptr<std::vector<char>> data)
    : m_bbox(bbox)
    , m_registry(pointSize, data)
{ }

void Sleeper::addPoint(PointInfo** toAddPtr)
{
    Roller roller(m_bbox);
    m_registry.put(toAddPtr, roller);
}

void Sleeper::getPoints(
        MultiResults& results,
        std::size_t depthBegin,
        std::size_t depthEnd)
{
    Roller roller(m_bbox);
    m_registry.getPoints(roller, results, depthBegin, depthEnd);
}

void Sleeper::getPoints(
        MultiResults& results,
        const BBox& query,
        std::size_t depthBegin,
        std::size_t depthEnd)
{
    Roller roller(m_bbox);
    m_registry.getPoints(roller, results, query, depthBegin, depthEnd);
}

std::shared_ptr<std::vector<char>> Sleeper::baseData()
{
    return m_registry.baseData();
}

BBox Sleeper::bbox() const
{
    return m_bbox;
}


/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/simple-point-table.hpp>

#include <entwine/types/schema.hpp>

namespace entwine
{

SimplePointTable::SimplePointTable(DataPool& dataPool, const Schema& schema)
    : SizedPointTable(schema.pdalLayout())
    , m_dataPool(dataPool)
    , m_dataNodes()
    , m_numPoints(0)
{ }

pdal::PointId SimplePointTable::addPoint()
{
    m_dataNodes.push_back(m_dataPool.acquire());
    return m_numPoints++;
}

char* SimplePointTable::getPoint(pdal::PointId index)
{
    return m_dataNodes.at(index)->val();
}

void SimplePointTable::setField(
        const pdal::Dimension::Detail* dimDetail,
        const pdal::PointId index,
        const void* value)
{
    std::memcpy(getDimension(dimDetail, index), value, dimDetail->size());
}

void SimplePointTable::getField(
        const pdal::Dimension::Detail* dimDetail,
        const pdal::PointId index,
        void* value)
{
    std::memcpy(value, getDimension(dimDetail, index), dimDetail->size());
}

char* SimplePointTable::getDimension(
        const pdal::Dimension::Detail* dimDetail,
        const pdal::PointId index)
{
    return getPoint(index) + dimDetail->offset();
}

void SimplePointTable::clear()
{
    m_dataNodes.clear();
    m_numPoints = 0;
}

std::size_t SimplePointTable::size() const
{
    return m_numPoints;
}

} // namespace entwine


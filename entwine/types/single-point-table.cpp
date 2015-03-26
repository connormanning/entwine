/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/single-point-table.hpp>

#include <entwine/types/schema.hpp>

namespace entwine
{

SinglePointTable::SinglePointTable(const Schema& schema, char* data)
    : SizedPointTable(schema.pdalLayout())
    , m_point(data)
{ }

pdal::PointId SinglePointTable::addPoint()
{
    throw std::runtime_error("Can't add a point to a SinglePointTable");
}

char* SinglePointTable::getPoint(pdal::PointId index)
{
    // All functions below go through this method, so validate here.
    if (index)
    {
        throw std::runtime_error("SinglePointTable only has one point");
    }

    return m_point;
}

void SinglePointTable::setField(
        const pdal::Dimension::Detail* dimDetail,
        const pdal::PointId index,
        const void* value)
{
    std::memcpy(getDimension(dimDetail, index), value, dimDetail->size());
}

void SinglePointTable::getField(
        const pdal::Dimension::Detail* dimDetail,
        const pdal::PointId index,
        void* value)
{
    std::memcpy(value, getDimension(dimDetail, index), dimDetail->size());
}

char* SinglePointTable::getDimension(
        const pdal::Dimension::Detail* dimDetail,
        const pdal::PointId index)
{
    return getPoint(index) + dimDetail->offset();
}

} // namespace entwine


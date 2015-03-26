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

SimplePointTable::SimplePointTable()
    : BasePointTable()
    , m_data()
    , m_numPoints(0)
{ }

SimplePointTable::SimplePointTable(const Schema& schema)
    : BasePointTable(schema.pdalLayout())
    , m_data()
    , m_numPoints(0)
{ }

SimplePointTable::SimplePointTable(
        const Schema& schema,
        const std::vector<char>& data)
    : BasePointTable(schema.pdalLayout())
    , m_data(data)
    , m_numPoints(data.size() / schema.pointSize())
{
    assert(m_data.size() % schema.pointSize() == 0);
}

pdal::PointId SimplePointTable::addPoint()
{
    m_data.resize(m_data.size() + m_layout->pointSize());
    return m_numPoints++;
}

char* SimplePointTable::getPoint(pdal::PointId index)
{
    return m_data.data() + index * m_layout->pointSize();
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
    m_data.clear();
    m_numPoints = 0;
}

std::size_t SimplePointTable::size() const
{
    return m_data.size();
}

std::vector<char> SimplePointTable::data() const
{
    return m_data;
}

} // namespace entwine


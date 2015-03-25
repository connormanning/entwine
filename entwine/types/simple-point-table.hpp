/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>
#include <vector>

#include <pdal/Dimension.hpp>
#include <pdal/PointTable.hpp>

namespace entwine
{

class Schema;

class SimplePointTable : public pdal::BasePointTable
{
public:
    SimplePointTable();
    explicit SimplePointTable(const Schema& schema);

    virtual pdal::PointId addPoint();
    virtual char* getPoint(pdal::PointId index);
    virtual void setField(
            const pdal::Dimension::Detail* dimDetail,
            pdal::PointId index,
            const void* value);
    virtual void getField(
            const pdal::Dimension::Detail* dimDetail,
            pdal::PointId index,
            void* value);

    void clear();

    std::vector<char> data() const;

private:
    char* getDimension(
            const pdal::Dimension::Detail* dimDetail,
            pdal::PointId index);

    std::vector<char> m_data;
    std::size_t m_numPoints;
};

} // namespace entwine


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

#include <entwine/types/sized-point-table.hpp>

namespace entwine
{

class Schema;

class SinglePointTable : public SizedPointTable
{
public:
    SinglePointTable(const Schema& schema, char* data);

    void setData(char* data) { m_point = data; }

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

    virtual std::size_t size() const { return 1; }

private:
    char* getDimension(
            const pdal::Dimension::Detail* dimDetail,
            pdal::PointId index);

    char* m_point;
};

} // namespace entwine


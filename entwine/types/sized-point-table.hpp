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

#include <pdal/Dimension.hpp>
#include <pdal/PointLayout.hpp>
#include <pdal/PointTable.hpp>

namespace entwine
{

class SizedPointTable : public pdal::SimplePointTable
{
public:
    explicit SizedPointTable(pdal::PointLayout& layout)
        : pdal::SimplePointTable(layout)
        , m_layout(layout)
    { }

    ~SizedPointTable() { }

    // Inherited from pdal::BasePointTable.
    virtual pdal::PointLayoutPtr layout() const { return &m_layout; }
    virtual pdal::PointId addPoint() = 0;
    virtual char* getPoint(pdal::PointId index) = 0;
    virtual void setField(
            const pdal::Dimension::Detail* dimDetail,
            pdal::PointId index,
            const void* value) = 0;
    virtual void getField(
            const pdal::Dimension::Detail* dimDetail,
            pdal::PointId index,
            void* value) = 0;

    virtual std::size_t size() const = 0;

protected:
    pdal::PointLayout& m_layout;
};

} // namespace entwine


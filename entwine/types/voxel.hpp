/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>

#include <entwine/types/point.hpp>
#include <entwine/types/scale-offset.hpp>

namespace entwine
{

class Voxel
{
public:
    const Point& point() const { return m_point; }
    const char* const data() const { return m_data; }
    void setData(char* pos) { m_data = pos; }

    void initDeep(const Point& point, const char* const pos, std::size_t size)
    {
        m_point = point;
        std::copy(pos, pos + size, m_data);
    }

    void initShallow(const pdal::PointRef& pr, char* pos)
    {
        m_point.x = pr.getFieldAs<double>(pdal::Dimension::Id::X);
        m_point.y = pr.getFieldAs<double>(pdal::Dimension::Id::Y);
        m_point.z = pr.getFieldAs<double>(pdal::Dimension::Id::Z);
        m_data = pos;
    }

    void clip(const ScaleOffset& so)
    {
        m_point = so.clip(m_point);
    }

private:
    Point m_point;
    char* m_data = nullptr;
};

} // namespace entwine


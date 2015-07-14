/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/point-info.hpp>

#include <pdal/Dimension.hpp>
#include <pdal/PointView.hpp>

#include <entwine/types/schema.hpp>

namespace entwine
{

PointInfo::PointInfo(const Point& point)
    : point(point)
    , data(0)
{ }

PointInfoShallow::PointInfoShallow(const Point& point, char* pos)
    : PointInfo(point)
{
    data = pos;
}

PointInfoDeep::PointInfoDeep(
        const Point& point,
        const char* pos,
        const std::size_t length)
    : PointInfo(point)
    , m_bytes(pos, pos + length)
{
    data = m_bytes.data();
}

} // namespace entwine


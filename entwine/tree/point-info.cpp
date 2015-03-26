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

#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>

namespace entwine
{

PointInfo::PointInfo(const Point* point, char* pos, const std::size_t len)
    : point(point)
    , bytes(len)
{
    std::memcpy(bytes.data(), pos, len);
}

void PointInfo::write(char* pos)
{
    std::memcpy(pos, bytes.data(), bytes.size());
}

} // namespace entwine


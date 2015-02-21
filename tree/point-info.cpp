/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "point-info.hpp"

#include <pdal/Dimension.hpp>
#include <pdal/PointBuffer.hpp>
#include <pdal/PointContext.hpp>

#include "types/point.hpp"

PointInfo::PointInfo(
        const pdal::PointContextRef pointContext,
        const pdal::PointBuffer* pointBuffer,
        const std::size_t index,
        const pdal::Dimension::Id::Enum originDim,
        const Origin origin)
    : point(new Point(
            pointBuffer->getFieldAs<double>(pdal::Dimension::Id::X, index),
            pointBuffer->getFieldAs<double>(pdal::Dimension::Id::Y, index)))
    , bytes(pointBuffer->pointSize())
{
    char* pos(bytes.data());
    for (const auto& dim : pointContext.dims())
    {
        // Not all dimensions may be present in every pipeline of our
        // invokation, which is not an error.
        if (pointBuffer->hasDim(dim))
        {
            pointBuffer->getRawField(dim, index, pos);
        }
        else if (dim == originDim)
        {
            std::memcpy(pos, &origin, sizeof(Origin));
        }

        pos += pointContext.dimSize(dim);
    }
}

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



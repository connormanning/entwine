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

PointInfo::PointInfo(
        const Schema& treeSchema,
        const pdal::PointView& remoteView,
        const std::size_t index,
        const Origin origin)
    : point(new Point(
            remoteView.getFieldAs<double>(pdal::Dimension::Id::X, index),
            remoteView.getFieldAs<double>(pdal::Dimension::Id::Y, index)))
    , bytes(treeSchema.pointSize())
{
    char* pos(bytes.data());

    for (const auto& dim : treeSchema.dims())
    {
        const pdal::Dimension::Id::Enum dimId(dim.id());

        // Not all dimensions may be present in every pipeline of our
        // invokation, which is not an error.
        if (remoteView.hasDim(dimId))
        {
            remoteView.getField(pos, dimId, dim.type(), index);
        }
        else if (dim.name() == "Origin")
        {
            std::memcpy(pos, &origin, sizeof(Origin));
        }

        pos += remoteView.dimSize(dimId);
    }

    // TODO Copy origin dimension.
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

} // namespace entwine


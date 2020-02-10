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

#include <entwine/types/point.hpp>

namespace entwine
{

struct ScaleOffset
{
    ScaleOffset() = default;
    ScaleOffset(Scale scale) : scale(scale) { }
    ScaleOffset(Scale scale, Offset offset)
        : scale(scale)
        , offset(offset)
    { }

    Scale scale = 1;
    Offset offset = 0;
};

inline Point clip(const Point& p, const ScaleOffset& so)
{
    return Point::unscale(
        Point::scale(p, so.scale, so.offset).round(),
        so.scale,
        so.offset);
}

} // namespace entwine

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

class ScaleOffset
{
public:
    ScaleOffset(const Scale& scale, const Offset& offset)
        : m_scale(scale)
        , m_offset(offset)
    { }

    const Scale& scale() const { return m_scale; }
    const Offset& offset() const { return m_offset; }

    Point clip(const Point& a) const
    {
        return Point::unscale(
            Point::scale(a, scale(), offset()).round(),
            scale(),
            offset());
    }

private:
    const Scale m_scale;
    const Offset m_offset = 1;
};

} // namespace entwine


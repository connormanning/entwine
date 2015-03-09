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

#include <pdal/PointContext.hpp>

#include <cstdint>
#include <vector>

typedef uint64_t Origin;

namespace pdal
{
    class PointBuffer;
}

namespace entwine
{

class Point;

class PointInfo
{
public:
    PointInfo(
            const pdal::PointContextRef pointContext,
            const pdal::PointBuffer* pointBuffer,
            std::size_t index,
            Origin origin);

    PointInfo(const Point* point, char* pos, std::size_t len);

    void write(char* pos);

    const Point* point;
    std::vector<char> bytes;
};

} // namespace entwine


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
#include <cstdint>
#include <vector>

namespace pdal
{
    class PointView;
}

namespace entwine
{

typedef uint64_t Origin;

class Point;
class Schema;

class PointInfo
{
public:
    PointInfo(const Point* point, char* pos, std::size_t len);

    void write(char* pos);

    const Point* point;
    std::vector<char> bytes;
};

} // namespace entwine


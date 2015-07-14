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

#include <entwine/types/point.hpp>

namespace pdal
{
    class PointView;
}

namespace entwine
{

class Schema;

class PointInfo
{
public:
    virtual ~PointInfo() { }

    Point point;
    const char* data;

protected:
    explicit PointInfo(const Point& point);
};

class PointInfoShallow : public PointInfo
{
public:
    PointInfoShallow(const Point& point, char* pos);
};

class PointInfoDeep : public PointInfo
{
public:
    PointInfoDeep(const Point& point, const char* pos, std::size_t length);

private:
    std::vector<char> m_bytes;
};

} // namespace entwine


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

#include <atomic>
#include <cstddef>
#include <vector>

#include <entwine/types/point.hpp>

namespace entwine
{

class PointInfo
{
public:
    PointInfo(const Point& point, const char* data, std::size_t size)
        : m_point(point)
        , m_data(data, data + size)
    { }

    const Point& point() const { return m_point; }
    const std::vector<char>& data() const { return m_data; }

private:
    const Point m_point;
    const std::vector<char> m_data;
};

class PointInfoShallow
{
public:
    PointInfoShallow(const Point& point, const char* pos)
        : m_point(point)
        , m_pos(pos)
    { }

    const Point& point() const { return m_point; }
    const char* data() const { return m_pos; }

private:
    const Point m_point;
    const char* m_pos;
};

} // namespace entwine


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
#include <entwine/third/splice-pool/splice-pool.hpp>

namespace entwine
{

typedef splicer::BufferPool<char> DataPool;
typedef DataPool::NodeType RawDataNode;
typedef DataPool::UniqueNodeType PooledDataNode;
typedef DataPool::UniqueStackType PooledDataStack;

class PointInfoShallow
{
public:
    PointInfoShallow() noexcept
        : m_point()
        , m_dataNode()
    { }

    PointInfoShallow(const Point& point, PooledDataNode dataNode) noexcept
        : m_point(point)
        , m_dataNode(std::move(dataNode))
    { }

    const Point& point() const { return m_point; }
    const char* data() const { return m_dataNode->val(); }

    PooledDataNode acquireDataNode() { return std::move(m_dataNode); }

private:
    const Point m_point;
    PooledDataNode m_dataNode;
};

class PointInfoNonPooled
{
public:
    PointInfoNonPooled(const Point& point, const char* pos)
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


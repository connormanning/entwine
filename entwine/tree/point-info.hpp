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
#include <entwine/util/object-pool.hpp>

namespace entwine
{

typedef BufferPool<char> DataPool;
typedef DataPool::NodeType PooledDataNode;
typedef DataPool::StackType PooledDataStack;

class PointInfoShallow
{
public:
    PointInfoShallow() noexcept
        : m_point()
        , m_dataNode(nullptr)
        , m_dataPool(nullptr)
    { }

    PointInfoShallow(
            const Point& point,
            PooledDataNode* dataNode,
            DataPool* dataPool) noexcept
        : m_point(point)
        , m_dataNode(dataNode)
        , m_dataPool(dataPool)
    { }

    ~PointInfoShallow()
    {
        if (m_dataPool) m_dataPool->release(m_dataNode);
    }

    const Point& point() const { return m_point; }
    const char* data() const { return m_dataNode->val(); }

    PooledDataNode* releaseDataNode()
    {
        if (m_dataPool)
        {
            m_dataPool = nullptr;
            return m_dataNode;
        }
        else
        {
            throw std::runtime_error("Tried to double release data node");
        }
    }

private:
    const Point m_point;
    PooledDataNode* m_dataNode;
    DataPool* m_dataPool;
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


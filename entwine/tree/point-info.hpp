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

#include <pdal/PointRef.hpp>

#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/third/bigint/little-big-int.hpp>
#include <entwine/third/splice-pool/splice-pool.hpp>

namespace entwine
{

typedef BigUint Id;

class PointInfo
{
public:
    virtual ~PointInfo() { }

    virtual const Point& point() const = 0;
    virtual const char* data() const = 0;
};

typedef splicer::BufferPool<char> DataPool;
typedef DataPool::NodeType RawDataNode;
typedef DataPool::UniqueNodeType PooledDataNode;
typedef DataPool::UniqueStackType PooledDataStack;

class PointInfoShallow : public PointInfo
{
public:
    PointInfoShallow() noexcept
        : m_point()
        , m_dataNode()
    { }

    PointInfoShallow(PooledDataNode dataNode) noexcept
        : m_point()
        , m_dataNode(std::move(dataNode))
    { }

    PointInfoShallow(const Point& point, PooledDataNode dataNode) noexcept
        : m_point(point)
        , m_dataNode(std::move(dataNode))
    { }

    void point(const pdal::PointRef& pointRef)
    {
        m_point = Point(
                pointRef.getFieldAs<double>(pdal::Dimension::Id::X),
                pointRef.getFieldAs<double>(pdal::Dimension::Id::Y),
                pointRef.getFieldAs<double>(pdal::Dimension::Id::Z));
    }

    virtual const Point& point() const override { return m_point; }
    virtual const char* data() const override { return m_dataNode->val(); }

    char* data() { return m_dataNode->val(); }

    PooledDataNode acquireDataNode() { return std::move(m_dataNode); }

private:
    Point m_point;
    PooledDataNode m_dataNode;
};

typedef splicer::ObjectPool<PointInfoShallow> InfoPool;
typedef InfoPool::NodeType RawInfoNode;
typedef InfoPool::UniqueNodeType PooledInfoNode;
typedef InfoPool::UniqueStackType PooledInfoStack;

class PointPool
{
public:
    PointPool(const Schema& schema)
        : m_schema(schema)
        , m_dataPool(schema.pointSize(), 4096 * 32)
        , m_infoPool(4096 * 32)
    { }

    const Schema& schema() { return m_schema; }
    DataPool& dataPool() { return m_dataPool; }
    InfoPool& infoPool() { return m_infoPool; }

private:
    const Schema& m_schema;

    DataPool m_dataPool;
    InfoPool m_infoPool;
};

class PointInfoNonPooled : public PointInfo
{
public:
    PointInfoNonPooled(const Point& point, const char* pos)
        : m_point(point)
        , m_pos(pos)
    { }

    virtual const Point& point() const override { return m_point; }
    virtual const char* data() const override { return m_pos; }

private:
    const Point m_point;
    const char* m_pos;
};

} // namespace entwine


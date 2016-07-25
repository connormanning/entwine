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
#include <cstring>

#include <pdal/PointRef.hpp>

#include <entwine/types/defs.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/third/splice-pool/splice-pool.hpp>

namespace entwine
{

class Data
{
public:
    using Pool = splicer::BufferPool<char>;
    using RawNode = Pool::NodeType;
    using RawStack = Pool::StackType;
    using PooledNode = Pool::UniqueNodeType;
    using PooledStack = Pool::UniqueStackType;

    Data() = delete;
};

class Cell
{
public:
    using Pool = splicer::ObjectPool<Cell>;
    using RawNode = Pool::NodeType;
    using RawStack = Pool::StackType;
    using PooledNode = Pool::UniqueNodeType;
    using PooledStack = Pool::UniqueStackType;

    Cell() noexcept : m_point(), m_dataStack() { }

    const Point& point() const { return m_point; }
    Data::RawStack&& acquire() { return std::move(m_dataStack); }

    void push(Cell::PooledNode&& other, std::size_t pointSize)
    {
        assert(point() == other->point());
        m_dataStack.push(
                other->m_dataStack,
                [pointSize](const char* a, const char* b)
                {
                    return std::memcmp(a, b, pointSize) < 0;
                });
    }

    std::size_t size() const { return m_dataStack.size(); }
    bool unique() const { return m_dataStack.size() == 1; }
    bool empty() const { return m_dataStack.empty(); }

    Data::RawStack::ConstIterator begin() const { return m_dataStack.cbegin(); }
    Data::RawStack::ConstIterator end() const { return m_dataStack.cend(); }

    char* uniqueData()
    {
        assert(unique());
        return **m_dataStack.head();
    }

    void set(const pdal::PointRef& pointRef, Data::PooledNode&& dataNode)
    {
        m_point = Point(
                pointRef.getFieldAs<double>(pdal::Dimension::Id::X),
                pointRef.getFieldAs<double>(pdal::Dimension::Id::Y),
                pointRef.getFieldAs<double>(pdal::Dimension::Id::Z));

        m_dataStack.push(dataNode.release());
    }

private:
    Point m_point;
    Data::RawStack m_dataStack;
};

class PointPool
{
public:
    PointPool(const Schema& schema)
        : m_schema(schema)
        , m_dataPool(schema.pointSize(), 4096 * 32)
        , m_cellPool(4096 * 32)
    { }

    const Schema& schema() { return m_schema; }
    Data::Pool& dataPool() { return m_dataPool; }
    Cell::Pool& cellPool() { return m_cellPool; }

private:
    const Schema& m_schema;

    Data::Pool m_dataPool;
    Cell::Pool m_cellPool;
};

} // namespace entwine


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
#include <map>
#include <memory>
#include <vector>

#include <entwine/types/point-pool.hpp>

namespace entwine
{

namespace arbiter { class Endpoint; }
class Bounds;
class Metadata;
class Schema;

class PointInfo
{
public:
    PointInfo(const Point& point, const char* data, uint64_t tick = 0)
        : m_point(point)
        , m_data(data)
        , m_tick(tick)
    { }

    PointInfo(uint64_t tick) : m_tick(tick) { }

    const Point& point() const { return m_point; }
    const char* data() const { return m_data; }
    uint64_t tick() const { return m_tick; }

    bool operator<(const PointInfo& other) const
    {
        return m_tick < other.m_tick;
    }

private:
    Point m_point;
    const char* m_data = nullptr;
    uint64_t m_tick = 0;
};

using TubeData = std::vector<PointInfo>;

// Ordered by Z-tick to perform the tubular-quadtree-as-octree query.
class ChunkReader
{
public:
    ChunkReader(
            const Metadata& metadata,
            const arbiter::Endpoint& endpoint,
            PointPool& pool,
            const Id& id,
            std::size_t depth);

    using It = TubeData::const_iterator;
    struct QueryRange
    {
        QueryRange(It begin, It end) : begin(begin), end(end) { }

        It begin;
        It end;
    };

    QueryRange candidates(const Bounds& queryBounds) const;
    std::size_t size() const
    {
        return m_cells.size() * m_schema.pointSize();
    }

    const Id& id() const { return m_id; }

private:
    const Schema& schema() const { return m_schema; }

    std::size_t normalize(const Id& rawIndex) const
    {
        return (rawIndex - m_id).getSimple();
    }

    const Schema& m_schema;
    const Bounds& m_bounds;
    const Id m_id;
    const std::size_t m_depth;

    Cell::PooledStack m_cells;
    TubeData m_points;
};

// Ordered by normal BaseChunk ordering for traversal.
class BaseChunkReader
{
public:
    BaseChunkReader(const Metadata& m, PointPool& pool);

    const TubeData& tubeData(const Id& id) const
    {
        return m_points.at(normalize(id));
    }

protected:
    std::size_t normalize(const Id& v) const { return (v - m_id).getSimple(); }

    const Id m_id;
    PointPool& m_pool;
    Cell::PooledStack m_cells;
    std::vector<TubeData> m_points;
};

class SlicedBaseChunkReader : public BaseChunkReader
{
public:
    SlicedBaseChunkReader(
            const Metadata& m,
            PointPool& pool,
            const arbiter::Endpoint& ep);
};

class CelledBaseChunkReader : public BaseChunkReader
{
public:
    CelledBaseChunkReader(
            const Metadata& m,
            PointPool& pool,
            const arbiter::Endpoint& ep);
};

} // namespace entwine


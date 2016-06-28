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

class Bounds;
class Metadata;
class Schema;

class PointInfo
{
public:
    PointInfo(const Point& point, const char* data)
        : m_point(point)
        , m_data(data)
    { }

    const Point& point() const { return m_point; }
    const char* data() const { return m_data; }

private:
    const Point m_point;
    const char* m_data;
};

// Ordered by Z-tick to perform the tubular-quadtree-as-octree query.
class ChunkReader
{
public:
    ChunkReader(
            const Schema& schema,
            const Bounds& bounds,
            const Id& id,
            std::size_t depth,
            std::unique_ptr<std::vector<char>> data);

    using PointMap = std::multimap<uint64_t, PointInfo>;
    using It = PointMap::const_iterator;

    struct QueryRange
    {
        QueryRange(It begin, It end) : begin(begin), end(end) { }

        It begin;
        It end;
    };

    QueryRange candidates(const Bounds& queryBounds) const;

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

    std::unique_ptr<std::vector<char>> m_data;
    PointMap m_points;
};

// Ordered by normal BaseChunk ordering for traversal.
class BaseChunkReader
{
public:
    BaseChunkReader(
            const Metadata& metadata,
            const Schema& celledSchema,
            const Id& id,
            std::unique_ptr<std::vector<char>> compressed);

    using TubeData = std::vector<PointInfo>;

    const TubeData& getTubeData(const Id& id) const
    {
        return m_tubes.at(normalize(id));
    }

private:
    std::size_t normalize(const Id& rawIndex) const
    {
        return (rawIndex - m_id).getSimple();
    }

    const Id m_id;
    std::unique_ptr<std::vector<char>> m_data;
    std::vector<TubeData> m_tubes;
};

} // namespace entwine


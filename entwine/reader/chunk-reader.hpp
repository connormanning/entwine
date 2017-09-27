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

#include <cassert>
#include <cstddef>
#include <map>
#include <memory>
#include <vector>

#include <entwine/reader/append.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/io.hpp>

namespace entwine
{

class Bounds;
class Metadata;
class Schema;

class ChunkReader
{
public:
    // Cold chunks.
    ChunkReader(
            const Metadata& metadata,
            const arbiter::Endpoint& endpoint,
            const Bounds& bounds,
            PointPool& pool,
            const Id& id,
            std::size_t depth);

    // Base chunks.
    ChunkReader(
            const Metadata& metadata,
            const arbiter::Endpoint& endpoint,
            PointPool& pool);

    ~ChunkReader();

    const Metadata& metadata() const { return m_metadata; }
    const Schema& schema() const { return m_schema; }
    const Id& id() const { return m_id; }
    std::size_t depth() const { return m_depth; }
    const Bounds& bounds() const { return m_bounds; }
    const Cell::PooledStack& cells() const { return m_cells; }
    const std::vector<std::size_t> offsets() const { return m_offsets; }

    Append& getOrCreateAppend(std::string name, const Schema& s) const
    {
        std::lock_guard<std::mutex> lock(m);
        if (!m_appends.count(name))
        {
            auto append = makeUnique<Append>(
                    m_endpoint,
                    name,
                    s,
                    m_id,
                    m_cells.size());
            m_appends[name] = std::move(append);
        }
        return *m_appends.at(name);
    }

    Append* findAppend(const std::string name, const Schema& s) const
    {
        std::lock_guard<std::mutex> lock(m);
        if (m_appends.count(name)) return m_appends.at(name).get();

        const auto np(m_cells.size());
        if (auto a = Append::maybeCreate(m_endpoint, name, s, m_id, np))
        {
            m_appends[name] = std::move(a);
            return m_appends.at(name).get();
        }
        else return nullptr;
    }

protected:
    void initLegacyBase();

    const arbiter::Endpoint m_endpoint;
    const Metadata& m_metadata;
    PointPool m_pool;
    const Bounds m_bounds;
    const Schema& m_schema;
    const Id m_id;
    const std::size_t m_depth;

    Cell::PooledStack m_cells;
    std::vector<std::size_t> m_offsets;

    mutable std::mutex m;
    mutable std::map<std::string, std::unique_ptr<Append>> m_appends;
};

class PointInfo
{
public:
    PointInfo(
            std::size_t offset,
            const Point& point,
            const char* data,
            uint64_t tick = 0)
        : m_offset(offset)
        , m_point(point)
        , m_data(data)
        , m_tick(tick)
    { }

    PointInfo(uint64_t tick) : m_tick(tick) { }

    const Point& point() const { return m_point; }
    const char* data() const { return m_data; }
    uint64_t tick() const { return m_tick; }
    std::size_t offset() const { return m_offset; }

    bool operator<(const PointInfo& other) const
    {
        return m_tick < other.m_tick;
    }

private:
    std::size_t m_offset = 0;
    Point m_point;
    const char* m_data = nullptr;
    uint64_t m_tick = 0;
};

using TubeData = std::vector<PointInfo>;

class ColdChunkReader
{
public:
    ColdChunkReader(
            const Metadata& m,
            const arbiter::Endpoint& ep,
            const Bounds& bounds,
            PointPool& pool,
            const Id& id,
            std::size_t depth);

    using It = TubeData::const_iterator;
    struct QueryRange
    {
        QueryRange(It begin, It end) : begin(begin), end(end) { }
        It begin, end;
    };

    QueryRange candidates(const Bounds& queryBounds) const;
    std::size_t size() const
    {
        return m_chunk.cells().size() * m_chunk.schema().pointSize();
    }

    ChunkReader& chunk() { return m_chunk; }
    ChunkReader& chunk() const { return m_chunk; }

private:
    mutable ChunkReader m_chunk;
    TubeData m_points;
};

class BaseChunkReader
{
public:
    BaseChunkReader(
            const Metadata& m,
            const arbiter::Endpoint& ep,
            PointPool& pool);

    const TubeData& tubeData(const Id& id) const
    {
        if (m_points.count(id)) return m_points.at(id);
        else return m_empty;
    }

    ChunkReader& chunk() { return m_chunk; }
    ChunkReader& chunk() const { return m_chunk; }

private:
    mutable ChunkReader m_chunk;
    std::map<Id, TubeData> m_points;
    const TubeData m_empty;
};

} // namespace entwine


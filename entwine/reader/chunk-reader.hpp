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

class Extra
{
public:
    Extra(
            const arbiter::Endpoint& ep,
            const Id& id,
            const std::string& name,
            const Schema& schema,
            std::size_t numPoints)
        : m_ep(ep)
        , m_id(id)
        , m_name(name)
        , m_schema(schema)
        , m_dimTypeList(m_schema.pdalLayout().dimTypes())
        , m_table(m_schema)
    {
        if (m_ep.tryGetSize(filename()))
        {
            m_table.data() = m_ep.getBinary(filename());
        }

        m_table.resize(numPoints);
    }

    char* get(std::size_t offset)
    {
        m_touched = true;
        return m_table.getPoint(offset);
    }

    std::string filename() const
    {
        return std::string("d/") + m_name + "-" + m_id.str();
    }

    bool touched() const { return m_touched; }
    VectorPointTable& table() { return m_table; }

    void write() const
    {
        if (touched()) std::cout << "Writing " << filename() << std::endl;
        if (touched()) io::ensurePut(m_ep, filename(), m_table.data());
    }

    const pdal::DimTypeList& dimTypeList() const { return m_dimTypeList; }

private:
    const arbiter::Endpoint& m_ep;
    const Id& m_id;
    const std::string m_name;

    const Schema m_schema;
    const pdal::DimTypeList m_dimTypeList;
    VectorPointTable m_table;
    bool m_touched = false;
};

// Ordered by Z-tick to perform the tubular-quadtree-as-octree query.
class ChunkReader
{
public:
    ChunkReader(
            const Metadata& metadata,
            const arbiter::Endpoint& endpoint,
            const Bounds& bounds,
            PointPool& pool,
            const Id& id,
            std::size_t depth);

    ~ChunkReader();

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

    Extra& extra(std::string name, const Schema& s) const
    {
        std::lock_guard<std::mutex> lock(m);
        if (!m_extras.count(name))
        {
            m_extras.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(name),
                    std::forward_as_tuple(
                        m_endpoint,
                        m_id,
                        name,
                        s,
                        m_cells.size()));
        }
        return m_extras.at(name);
    }

private:
    const Schema& schema() const { return m_schema; }

    std::size_t normalize(const Id& rawIndex) const
    {
        return (rawIndex - m_id).getSimple();
    }

    const arbiter::Endpoint m_endpoint;
    const Metadata& m_metadata;
    PointPool& m_pool;
    const Bounds m_bounds;
    const Schema& m_schema;
    const Id m_id;
    const std::size_t m_depth;

    Cell::PooledStack m_cells;
    TubeData m_points;

    mutable std::mutex m;
    mutable std::map<std::string, Extra> m_extras;
};

class BaseExtra
{
public:
    BaseExtra(
            const arbiter::Endpoint& ep,
            std::string name,
            const Schema& schema,
            std::size_t baseDepthBegin,
            const std::vector<std::size_t>& slices)
        : m_ep(ep)
        , m_name(name)
        , m_schema(schema)
        , m_dimTypeList(m_schema.pdalLayout().dimTypes())
        , m_baseDepthBegin(baseDepthBegin)
    {
        std::size_t d(m_baseDepthBegin);
        for (const auto s : slices)
        {
            m_tables.emplace_back(makeUnique<VectorPointTable>(schema));
            auto& t(*m_tables.back());

            if (m_ep.tryGetSize(filename(d)))
            {
                std::cout << "Awakening base extra " << d << std::endl;
                t.data() = m_ep.getBinary(filename(d));

                if (t.data().size() != s)
                {
                    throw std::runtime_error("Invalid base extra size");
                }
            }

            t.resize(s);
            ++d;
        }
    }

    std::string filename(std::size_t d) const
    {
        return std::string("d/") + m_name + "-" +
            ChunkInfo::calcLevelIndex(2, d).str();
    }

    void write() const
    {
        if (!m_touched) return;

        std::size_t d(m_baseDepthBegin);
        for (const auto& t : m_tables)
        {
            std::cout << "Writing base " << filename(d) << std::endl;
            io::ensurePut(m_ep, filename(d), t->data());
            ++d;
        }
    }

    char* get(std::size_t depth, std::size_t offset)
    {
        m_touched = true;
        return table(depth).getPoint(offset);
    }

    VectorPointTable& table(std::size_t depth)
    {
        return *m_tables.at(depth - m_baseDepthBegin);
    }

    const Schema& schema() const { return m_schema; }
    const pdal::DimTypeList& dimTypeList() const { return m_dimTypeList; }

private:
    const arbiter::Endpoint& m_ep;
    const std::string m_name;
    const Schema m_schema;
    const pdal::DimTypeList m_dimTypeList;
    const std::size_t m_baseDepthBegin;
    std::vector<std::unique_ptr<VectorPointTable>> m_tables;

    bool m_touched = false;
};

// Ordered by normal BaseChunk ordering for traversal.
class BaseChunkReader
{
public:
    BaseChunkReader(
            const Metadata& m,
            const arbiter::Endpoint& ep,
            PointPool& pool);
    ~BaseChunkReader();

    const TubeData& tubeData(const Id& id) const
    {
        return m_points.at(normalize(id));
    }

    BaseExtra& extra(std::string name, const Schema& s) const
    {
        std::lock_guard<std::mutex> lock(m);
        if (!m_extras.count(name))
        {
            m_extras.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(name),
                    std::forward_as_tuple(
                        m_ep,
                        name,
                        s,
                        m_baseDepthBegin,
                        m_slices));
        }
        return m_extras.at(name);
    }

    const std::map<std::string, BaseExtra>& extras() const { return m_extras; }

protected:
    std::size_t normalize(const Id& v) const { return (v - m_id).getSimple(); }

    const arbiter::Endpoint& m_ep;
    const Id m_id;
    PointPool& m_pool;
    Cell::PooledStack m_cells;
    std::vector<TubeData> m_points;

    std::size_t m_baseDepthBegin = 0;
    std::vector<std::size_t> m_slices;
    mutable std::mutex m;
    mutable std::map<std::string, BaseExtra> m_extras;
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


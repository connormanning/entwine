/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

/*
#pragma once

#include <cassert>
#include <cstddef>
#include <iomanip>
#include <iostream>

#include <entwine/tree/hierarchy.hpp>
#include <entwine/tree/key.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/dir.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class NewState
{
public:
    NewState(const Metadata& metadata, Origin origin)
        : m_metadata(metadata)
        , m_structure(m_metadata.structure())
        , m_origin(origin)
        , m_point(m_metadata)
        , m_chunk(m_metadata)
    { }

    void reset()
    {
        m_d = 0;
        m_point.reset();
        m_chunk.reset();
    }

    void init(const Point& p)
    {
        init(p, m_structure.head());
    }

    void init(const Point& p, uint64_t depth)
    {
        reset();
        while (m_d < depth) step(p);
    }

    void step(const Point& p)
    {
        m_point.step(p);
        if (m_d >= m_structure.body() && m_d < m_structure.tail())
        {
            m_chunk.step(p);
        }

        ++m_d;
    }

    Origin origin() const { return m_origin; }
    uint64_t depth() const { return m_d; }
    const Key& pointKey() const { return m_point; }
    const Key& chunkKey() const { return m_chunk; }

private:
    const Metadata& m_metadata;
    const Structure& m_structure;
    const Origin m_origin;

    uint64_t m_d = 0;

    Key m_point;
    Key m_chunk;
};

class PointState
{
public:
    PointState(
            const Structure& structure,
            const Bounds& bounds,
            std::size_t depth = 0)
        : m_structure(structure)
        , m_boundsOriginal(bounds)
        , m_bounds(bounds)
        , m_index(0)
        , m_depth(depth)
        , m_tick(0)
        , m_chunkId(m_structure.nominalChunkIndex())
        , m_chunkNum(0)
        , m_pointsPerChunk(m_structure.basePointsPerChunk())
        , m_chunkBounds(bounds)
    {
        reset();
    }

    void init(const Point& p)
    {
        reset();
        for (std::size_t d(0); d < m_structure.startDepth(); ++d) step(p);
    }

    virtual ~PointState() { }

    void climb(Dir dir)
    {
        climb(m_bounds.get(dir).mid());
    }

    virtual void climb(const Point& point)
    {
        if (++m_depth <= m_structure.startDepth()) return;

        const std::size_t workingDepth(depth());
        const Dir dir(getDirection(m_bounds.mid(), point));
        m_bounds.go(dir);

        if (m_structure.tubular() && workingDepth <= Tube::maxTickDepth())
        {
            m_tick <<= 1;
            if (isUp(dir)) ++m_tick;
        }

        m_index <<= m_structure.dimensions();
        ++m_index.data().front();
        m_index += toIntegral(dir, m_structure.tubular());

        if (workingDepth > m_structure.nominalChunkDepth())
        {
            chunkClimb(workingDepth, point);
        }
    }

    PointState getClimb(Dir dir) const
    {
        PointState s(*this);
        s.climb(dir);
        return s;
    }

    virtual void reset()
    {
        m_bounds = m_boundsOriginal;
        m_index = 0;
        m_depth = 0;
        m_tick = 0;

        m_chunkId = m_structure.nominalChunkIndex();
        m_chunkNum = 0;
        m_pointsPerChunk = m_structure.basePointsPerChunk();
        m_chunkBounds = m_boundsOriginal;
    }

    const Bounds& bounds() const { return m_bounds; }
    const Id& index() const     { return m_index; }
    std::size_t depth() const   { return m_depth - m_structure.startDepth(); }
    std::size_t tick() const    { return m_tick; }

    const Bounds& chunkBounds() const { return m_chunkBounds; }
    const Id& chunkId() const   { return m_chunkId; }
    const Id& pointsPerChunk() const    { return m_pointsPerChunk; }
    std::size_t chunkNum() const
    {
        if (m_chunkNum.trivial()) return m_chunkNum.getSimple();
        else return std::numeric_limits<std::size_t>::max();
    }

    void climbTo(const Point& point, std::size_t requestedDepth)
    {
        while (depth() < requestedDepth) climb(point);
    }

protected:
    void chunkClimb(std::size_t workingDepth, const Point& point)
    {
        if (workingDepth <= m_structure.sparseDepthBegin())
        {
            const Dir dir(getDirection(m_chunkBounds.mid(), point, true));
            m_chunkBounds.go(dir, true);

            m_chunkId <<= m_structure.dimensions();
            ++m_chunkId.data().front();

            m_chunkId += toIntegral(dir) * m_pointsPerChunk;

            if (workingDepth >= m_structure.coldDepthBegin())
            {
                m_chunkNum =
                    (m_chunkId - m_structure.coldIndexBegin()) /
                    m_pointsPerChunk;
            }
        }
        else
        {
            m_chunkNum += m_structure.maxChunksPerDepth();

            m_chunkId <<= m_structure.dimensions();
            ++m_chunkId.data().front();

            m_pointsPerChunk *= m_structure.factor();
        }
    }

    const Structure& m_structure;
    const Bounds& m_boundsOriginal;

    Bounds m_bounds;
    Id m_index;
    std::size_t m_depth;
    std::size_t m_tick;

    Id m_chunkId;
    Id m_chunkNum;
    Id m_pointsPerChunk;
    Bounds m_chunkBounds;
};

class HierarchyState : public PointState
{
    using CellCache = std::map<std::size_t, int>;

    class ColdPosition
    {
    public:
        ColdPosition() : m_id(0), m_tick(0), m_delta(0), m_cell(nullptr) { }
        ~ColdPosition() { count(); }

        bool tryCount(const Id& id, std::size_t tick, int delta)
        {
            if (id == m_id && tick == m_tick)
            {
                assert(m_cell);
                m_delta += delta;
                return true;
            }
            else
            {
                return false;
            }
        }

        void set(const Id& id, std::size_t tick, HierarchyCell& cell)
        {
            count();

            m_id = id;
            m_tick = tick;
            m_cell = &cell;
        }

    private:
        void count()
        {
            if (m_cell && m_delta)
            {
                m_cell->count(m_delta);
                m_delta = 0;
            }
        }

        Id m_id;
        std::size_t m_tick;
        int m_delta;
        HierarchyCell* m_cell;
    };

public:
    HierarchyState(
            const Metadata& metadata,
            Hierarchy* hierarchy,
            bool cache = true)
        : PointState(
                metadata.hierarchyStructure(),
                metadata.boundsScaledCubic())
        , m_hierarchy(hierarchy)
        , m_baseCache(m_hierarchy && cache && false ?
                std::vector<CellCache>(m_structure.baseIndexSpan()) :
                std::vector<CellCache>())
        , m_coldCache(m_hierarchy && cache ? 32 : 0)
    { }

    ~HierarchyState()
    {
        for (std::size_t i(0); i < m_baseCache.size(); ++i)
        {
            for (const auto& tube : m_baseCache[i])
            {
                m_hierarchy->countBase(i, tube.first, tube.second);
            }
        }
    }

    void count(int delta)
    {
        if (m_hierarchy && m_depth >= m_structure.startDepth())
        {
            const std::size_t workingDepth(depth());

            if (m_baseCache.size() && m_structure.isWithinBase(workingDepth))
            {
                auto& tube(m_baseCache[index().getSimple()]);

                if (tube.count(tick())) tube[tick()] += delta;
                else tube[tick()] = delta;
            }
            else
            {
                const std::size_t coldDepth(
                        workingDepth - m_structure.coldDepthBegin());

                if (coldDepth < m_coldCache.size())
                {
                    ColdPosition& entry(m_coldCache[coldDepth]);

                    if (!entry.tryCount(m_index, m_tick, delta))
                    {
                        entry.set(
                                m_index,
                                m_tick,
                                m_hierarchy->count(*this, delta));
                    }
                }
                else
                {
                    m_hierarchy->count(*this, delta);
                }
            }
        }
    }

private:
    Hierarchy* m_hierarchy;

    std::vector<CellCache> m_baseCache;
    std::vector<ColdPosition> m_coldCache;
};

class ChunkState : public PointState
{
    using PointState::climb;

public:
    ChunkState(const Metadata& metadata)
        : ChunkState(metadata.structure(), metadata.boundsScaledCubic())
    { }

    ChunkState(const Structure& structure, const Bounds& bounds)
        : PointState(structure, bounds)
    {
        m_depth = m_structure.nominalChunkDepth();
    }

    virtual void climb(const Point& point) override
    {
        chunkClimb(++m_depth, point);
    }

    virtual void reset() override
    {
        PointState::reset();
        m_depth = m_structure.nominalChunkDepth();
    }

    bool sparse() const
    {
        // True if the next call to climb() will fall into the sparse logic.
        return m_depth >= m_structure.sparseDepthBegin();
    }

    ChunkState getChunkClimb(Dir dir) const
    {
        ChunkState s(*this);
        s.climb(m_chunkBounds.get(dir).mid());
        return s;
    }
};

class Climber
{
public:
    Climber(
            const Metadata& metadata,
            const Origin origin = 0,
            Hierarchy* hierarchy = nullptr,
            bool cache = true)
        : m_metadata(metadata)
        , m_pointState(metadata.structure(), metadata.boundsScaledCubic())
        , m_hierarchyState(metadata, hierarchy, cache)
        , m_state(metadata, origin)
    { }

    void reset()
    {
        m_pointState.reset();
        m_hierarchyState.reset();
    }

    void init(const Point& point) { m_state.init(point); }
    void step(const Point& point) { m_state.step(point); }

    void count(int delta = 1) { m_hierarchyState.count(delta); }

    void magnify(const Point& point)
    {
        m_pointState.climb(point);
        m_hierarchyState.climb(point);
    }

    void magnifyTo(const Point& point, std::size_t depth)
    {
        while (m_pointState.depth() < depth) magnify(point);
    }

    void magnifyTo(const Bounds& bounds)
    {
        Bounds norm(bounds.min(), bounds.max());
        norm.growBy(.01);
        while (!norm.contains(m_pointState.bounds(), true)) magnify(norm.mid());
    }

    const PointState& pointState() const { return m_pointState; }

    const Id& index()   const { return m_pointState.index(); }
    std::size_t tick()  const { return m_pointState.tick(); }
    std::size_t depth() const { return m_pointState.depth(); }
    const Bounds& bounds()  const { return m_pointState.bounds(); }

    const Id& chunkId() const { return m_pointState.chunkId(); }
    const Id& pointsPerChunk() const { return m_pointState.pointsPerChunk(); }
    std::size_t chunkNum() const { return m_pointState.chunkNum(); }
    const Bounds& chunkBounds() const { return m_pointState.chunkBounds(); }

    std::size_t pointSize() const { return m_metadata.schema().pointSize(); }

    NewState& state() { return m_state; }
    const NewState& state() const { return m_state; }

private:
    const Metadata& m_metadata;

    PointState m_pointState;
    HierarchyState m_hierarchyState;

    NewState m_state;
};

class CellState
{
public:
    CellState(
            const Climber& climber,
            Cell::PooledNode&& cell,
            const std::vector<Origin>& origins)
        : m_climber(climber)
        , m_cell(std::move(cell))
        , m_origins(origins)
    { }

    Climber& climber() { return m_climber; }
    Cell::PooledNode acquireCellNode() { return std::move(m_cell); }
    const std::vector<Origin>& origins() const { return m_origins; }

private:
    Climber m_climber;
    Cell::PooledNode m_cell;
    const std::vector<Origin> m_origins;
};

} // namespace entwine

*/

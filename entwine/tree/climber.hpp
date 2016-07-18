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
#include <iomanip>
#include <iostream>

#include <entwine/tree/hierarchy.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/dir.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/tube.hpp>

namespace entwine
{

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
    { }

    virtual ~PointState() { }

    virtual void climb(Dir dir)
    {
        if (++m_depth <= m_structure.startDepth()) return;

        const std::size_t workingDepth(depth());

        if (m_structure.tubular() && workingDepth <= Tube::maxTickDepth())
        {
            m_tick <<= 1;
            if (isUp(dir)) ++m_tick;
        }

        m_index <<= m_structure.dimensions();
        m_index.incSimple();
        m_index += toIntegral(dir, m_structure.tubular());

        m_bounds.go(dir);

        if (workingDepth > m_structure.nominalChunkDepth())
        {
            chunkClimb(workingDepth);
        }
    }

    virtual void climb(const Point& point)
    {
        climb(getDirection(m_bounds.mid(), point));
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
    }

    const Bounds& bounds() const { return m_bounds; }
    const Id& index() const     { return m_index; }
    std::size_t depth() const   { return m_depth - m_structure.startDepth(); }
    std::size_t tick() const    { return m_tick; }

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
    void chunkClimb(std::size_t workingDepth)
    {
        if (workingDepth <= m_structure.sparseDepthBegin())
        {
            m_chunkId <<= m_structure.dimensions();
            m_chunkId.incSimple();
            m_chunkId += toIntegral(chunkDir()) * m_pointsPerChunk;

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
            m_chunkId.incSimple();

            m_pointsPerChunk *= m_structure.factor();
        }
    }

    virtual Dir chunkDir(Dir dir = Dir::swd) const
    {
        return toDir(
                (m_index - m_chunkId).getSimple() /
                m_pointsPerChunk.getSimple());
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
};

class HierarchyState : public PointState
{
public:
    HierarchyState(const Metadata& metadata, Hierarchy* hierarchy)
        : PointState(metadata.hierarchyStructure(), metadata.bounds())
        , m_hierarchy(hierarchy)
    { }

    void count(int delta)
    {
        if (m_hierarchy) m_hierarchy->count(*this, delta);
    }

private:
    Hierarchy* m_hierarchy;
};

class ChunkState : public PointState
{
public:
    ChunkState(const Metadata& metadata)
        : PointState(metadata.structure(), metadata.bounds())
        , m_boundsChunk(metadata.bounds())
    {
        m_depth = m_structure.nominalChunkDepth();
    }

    virtual void climb(Dir dir) override
    {
        chunkClimb(++m_depth);
        m_boundsChunk.go(dir, m_structure.tubular());
    }

    virtual Dir chunkDir(Dir dir = Dir::swd) const override { return dir; }

    virtual void climb(const Point& point) override
    {
        climb(getDirection(m_boundsChunk.mid(), point));
    }

    virtual void reset() override
    {
        PointState::reset();
        m_boundsChunk = m_boundsOriginal;
        m_depth = m_structure.nominalChunkDepth();
    }

    const Bounds& boundsChunk() const { return m_boundsChunk; }

    bool sparse() const
    {
        // True if the next call to climb() will fall into the sparse logic.
        return m_depth >= m_structure.sparseDepthBegin();
    }

    ChunkState getChunkClimb(Dir dir) const
    {
        ChunkState s(*this);
        s.climb(dir);
        return s;
    }

private:
    Bounds m_boundsChunk;
};

class Climber
{
public:
    Climber(const Metadata& metadata, Hierarchy* hierarchy = nullptr)
        : m_metadata(metadata)
        , m_pointState(metadata.structure(), metadata.bounds())
        , m_hierarchyState(metadata, hierarchy)
    { }

    void reset()
    {
        m_pointState.reset();
        m_hierarchyState.reset();
    }

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

private:
    const Metadata& m_metadata;

    PointState m_pointState;
    HierarchyState m_hierarchyState;
};

class CellState
{
public:
    CellState(const Climber& climber, Cell::PooledNode&& cell)
        : m_climber(climber)
        , m_cell(std::move(cell))
    { }

    Climber& climber() { return m_climber; }
    Cell::PooledNode acquireCellNode() { return std::move(m_cell); }

private:
    Climber m_climber;
    Cell::PooledNode m_cell;
};

} // namespace entwine


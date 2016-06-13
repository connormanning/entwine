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
#include <entwine/types/bbox.hpp>
#include <entwine/types/dir.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class PointState
{
public:
    PointState(
            const Structure& structure,
            const BBox& bbox,
            std::size_t startDepth = 0);

    virtual ~PointState() { }

    void climb(Dir dir);
    void climb(const Point& point) { climb(getDirection(point, m_bbox.mid())); }

    PointState getClimb(Dir dir) const
    {
        PointState pointState(*this);
        pointState.climb(dir);
        return pointState;
    }

    void reset()
    {
        m_bbox = m_bboxOriginal;
        m_index = 0;
        m_depth = 0;
        m_tick = 0;

        m_chunkId = m_structure.nominalChunkIndex();
        m_chunkNum = 0;
        m_pointsPerChunk = m_structure.basePointsPerChunk();
    }

    const BBox& bbox() const    { return m_bbox; }
    const Id& index() const     { return m_index; }
    std::size_t depth() const   { return m_depth - m_startDepth; }
    std::size_t tick() const    { return m_tick; }

    const Id& chunkId() const   { return m_chunkId; }
    const Id& pointsPerChunk() const    { return m_pointsPerChunk; }
    std::size_t chunkNum() const
    {
        if (m_chunkNum.trivial()) return m_chunkNum.getSimple();
        else return std::numeric_limits<std::size_t>::max();
    }

protected:
    const Structure& m_structure;
    const BBox& m_bboxOriginal;
    const std::size_t m_startDepth;

    BBox m_bbox;
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
        : PointState(
                metadata.hierarchyStructure(),
                metadata.bbox(),
                Hierarchy::startDepth())
        , m_hierarchy(hierarchy)
    { }

    void count(int delta)
    {
        if (m_hierarchy) m_hierarchy->count(*this, delta);
    }

private:
    Hierarchy* m_hierarchy;
};

class Climber
{
public:
    Climber(const Metadata& metadata, Hierarchy* hierarchy = nullptr)
        : m_metadata(metadata)
        , m_pointState(metadata.structure(), metadata.bbox())
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

    void magnifyTo(const BBox& bbox)
    {
        BBox norm(bbox.min(), bbox.max(), m_metadata.structure().is3d());
        norm.growBy(.01);
        while (!norm.contains(m_pointState.bbox(), true)) magnify(norm.mid());
    }

    const Id& index()   const { return m_pointState.index(); }
    std::size_t tick()  const { return m_pointState.tick(); }
    std::size_t depth() const { return m_pointState.depth(); }
    const BBox& bbox()  const { return m_pointState.bbox(); }

    const Id& chunkId() const { return m_pointState.chunkId(); }
    const Id& pointsPerChunk() const { return m_pointState.pointsPerChunk(); }
    std::size_t chunkNum() const { return m_pointState.chunkNum(); }

private:
    const Metadata& m_metadata;

    PointState m_pointState;
    HierarchyState m_hierarchyState;
    // OHierarchyState m_ohierarchyState;
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





class OHierarchyState
{
public:
    OHierarchyState(const Metadata& metadata, OHierarchy* hierarchy);

    void climb(const Point& point)
    {
        if (m_climber && ++m_depth > m_climber->depthBegin())
        {
            m_climber->magnify(point);
        }
    }

    void reset() { if (m_climber) m_climber->reset(); }
    void count() { if (m_depth >= m_climber->depthBegin()) m_climber->count(); }

private:
    std::size_t m_depth;
    std::unique_ptr<HierarchyClimber> m_climber;
};

} // namespace entwine


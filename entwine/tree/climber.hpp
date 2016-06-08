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
    PointState(const Metadata& metadata);
    PointState(const PointState&);
    PointState& operator=(const PointState&);

    void climb(const Point& point);
    void reset()
    {
        m_bbox = m_metadata.bbox();
        m_index = 0;
        m_depth = 0;
        m_tick = 0;
    }

    const BBox& bbox() const    { return m_bbox; }
    const Id& index() const     { return m_index; }
    std::size_t depth() const   { return m_depth; }
    std::size_t tick() const    { return m_tick; }

private:
    const Metadata& m_metadata;
    const Structure& m_structure;

    BBox m_bbox;
    Id m_index;
    std::size_t m_depth;
    std::size_t m_tick;
};

class ChunkState
{
public:
    ChunkState(const Metadata& metadata, const PointState& pointState);
    ChunkState(const ChunkState&, const PointState& pointState);
    ChunkState& operator=(const ChunkState&);

    void climb();
    void reset()
    {
        m_bbox = m_metadata.bbox();
        m_chunkId = m_structure.nominalChunkIndex();
        m_depth = 0;
        m_chunkNum = 0;
        m_pointsPerChunk = m_structure.basePointsPerChunk();
        m_chunksAtDepth = 1;
    }

    const BBox& bbox() const    { return m_bbox; }
    const Id& chunkId() const   { return m_chunkId; }
    const Id& pointsPerChunk() const    { return m_pointsPerChunk; }
    std::size_t chunksAtDepth() const   { return m_chunksAtDepth; }

    std::size_t chunkNum() const
    {
        if (m_chunkNum.trivial()) return m_chunkNum.getSimple();
        else return std::numeric_limits<std::size_t>::max();
    }

private:
    const Metadata& m_metadata;
    const Structure& m_structure;
    const PointState& m_pointState;

    BBox m_bbox;
    Id m_chunkId;
    std::size_t m_depth;

    Id m_chunkNum;
    Id m_pointsPerChunk;
    std::size_t m_chunksAtDepth;
};

class HierarchyState
{
public:
    HierarchyState(const Metadata& metadata, Hierarchy* hierarchy);
    HierarchyState(const HierarchyState&);
    HierarchyState& operator=(const HierarchyState&);

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

class Climber
{
public:
    Climber(const Metadata& metadata, Hierarchy* hierarchy = nullptr)
        : m_metadata(metadata)
        , m_pointState(metadata)
        , m_chunkState(metadata, m_pointState)
        , m_hierarchyState(metadata, hierarchy)
    { }

    Climber(const Climber& other)
        : m_metadata(other.m_metadata)
        , m_pointState(other.m_pointState)
        , m_chunkState(other.m_chunkState, m_pointState)
        , m_hierarchyState(other.m_hierarchyState)
    { }

    Climber& operator=(const Climber& other)
    {
        m_pointState = other.m_pointState;
        m_chunkState = other.m_chunkState;
        m_hierarchyState = other.m_hierarchyState;

        return *this;
    }

    void reset()
    {
        m_pointState.reset();
        m_chunkState.reset();
        m_hierarchyState.reset();
    }

    void count() { m_hierarchyState.count(); }

    void magnify(const Point& point)
    {
        m_chunkState.climb();
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

    const Id& chunkId() const { return m_chunkState.chunkId(); }
    const BBox& bboxChunk() const { return m_chunkState.bbox(); }
    const Id& pointsPerChunk() const { return m_chunkState.pointsPerChunk(); }
    std::size_t chunkNum() const { return m_chunkState.chunkNum(); }

private:
    const Metadata& m_metadata;

    PointState m_pointState;
    ChunkState m_chunkState;
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


/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/climber.hpp>

#include <cassert>

#include <entwine/types/tube.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

PointState::PointState(const Metadata& metadata)
    : m_metadata(metadata)
    , m_structure(m_metadata.structure())
    , m_bbox(m_metadata.bbox())
    , m_index(0)
    , m_depth(0)
    , m_tick(0)
{ }

PointState::PointState(const PointState& other)
    : m_metadata(other.m_metadata)
    , m_structure(other.m_structure)
    , m_bbox(other.m_bbox)
    , m_index(other.m_index)
    , m_depth(other.m_depth)
    , m_tick(other.m_tick)
{ }

PointState& PointState::operator=(const PointState& other)
{
    assert(&m_metadata == &other.m_metadata);

    m_bbox = other.m_bbox;
    m_index = other.m_index;
    m_depth = other.m_depth;
    m_tick = other.m_tick;

    return *this;
}

void PointState::climb(const Point& point)
{
    ++m_depth;

    const Point& mid(m_bbox.mid());
    if (m_structure.tubular() && m_depth <= Tube::maxTickDepth())
    {
        m_tick <<= 1;
        if (point.z >= mid.z) ++m_tick;
    }

    m_index <<= m_structure.dimensions();
    m_index.incSimple();

    const Dir dir(getDirection(point, mid));
    m_index += m_structure.tubular() ? toIntegral(dir) % 4 : toIntegral(dir);

    m_bbox.go(dir);
}

ChunkState::ChunkState(const Metadata& metadata, const PointState& pointState)
    : m_metadata(metadata)
    , m_structure(m_metadata.structure())
    , m_pointState(pointState)
    , m_bbox(m_metadata.bbox())
    , m_chunkId(m_structure.nominalChunkIndex())
    , m_depth(0)
    , m_chunkNum(0)
    , m_pointsPerChunk(m_structure.basePointsPerChunk())
    , m_chunksAtDepth(1)
{ }

ChunkState::ChunkState(const ChunkState& other, const PointState& pointState)
    : m_metadata(other.m_metadata)
    , m_structure(other.m_structure)
    , m_pointState(pointState)
    , m_bbox(other.m_bbox)
    , m_chunkId(other.m_chunkId)
    , m_depth(other.m_depth)
    , m_chunkNum(other.m_chunkNum)
    , m_pointsPerChunk(other.m_pointsPerChunk)
    , m_chunksAtDepth(other.m_chunksAtDepth)
{ }

ChunkState& ChunkState::operator=(const ChunkState& other)
{
    assert(&m_metadata == &other.m_metadata);

    m_bbox = other.m_bbox;
    m_chunkId = other.m_chunkId;
    m_depth = other.m_depth;
    m_chunkNum = other.m_chunkNum;
    m_pointsPerChunk = other.m_pointsPerChunk;
    m_chunksAtDepth = other.m_chunksAtDepth;

    return *this;
}

void ChunkState::climb()
{
    if (++m_depth <= m_structure.nominalChunkDepth()) return;

    if (
            m_depth <= m_structure.sparseDepthBegin() ||
            !m_structure.sparseDepthBegin())
    {
        const std::size_t chunkRatio(
                (m_pointState.index() - m_chunkId).getSimple() /
                (m_pointsPerChunk.getSimple() / m_structure.factor()));

        assert(chunkRatio < m_structure.factor());

        m_chunkId <<= m_structure.dimensions();
        m_chunkId.incSimple();
        m_chunkId += chunkRatio * m_pointsPerChunk;

        m_bbox.go(toDir(chunkRatio), m_structure.tubular());

        if (m_depth >= m_structure.coldDepthBegin())
        {
            m_chunkNum =
                (m_chunkId - m_structure.coldIndexBegin()) / m_pointsPerChunk;
        }

        m_chunksAtDepth *= m_structure.factor();
    }
    else
    {
        m_chunkNum += m_chunksAtDepth;

        m_chunkId <<= m_structure.dimensions();
        m_chunkId.incSimple();

        m_pointsPerChunk *= m_structure.factor();
    }
}

HierarchyState::HierarchyState(const Metadata& metadata, Hierarchy* hierarchy)
    : m_depth(0)
    , m_climber(
        hierarchy ?
            new HierarchyClimber(
                *hierarchy,
                metadata.structure().tubular() ||
                metadata.structure().dimensions() == 3 ? 3 : 2) :
            nullptr)
{ }

HierarchyState::HierarchyState(const HierarchyState& other)
    : m_depth(other.m_depth)
    , m_climber(
            other.m_climber ?
                makeUnique<HierarchyClimber>(*other.m_climber) :
                nullptr)
{ }

HierarchyState& HierarchyState::operator=(const HierarchyState& other)
{
    m_depth = other.m_depth;
    if (other.m_climber)
    {
        m_climber = makeUnique<HierarchyClimber>(*other.m_climber);
    }

    return *this;
}

} // namespace entwine


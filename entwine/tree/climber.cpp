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

PointState::PointState(
        const Structure& structure,
        const BBox& bbox,
        const std::size_t startDepth)
    : m_structure(structure)
    , m_bboxOriginal(bbox)
    , m_startDepth(startDepth)
    , m_bbox(bbox)
    , m_index(0)
    , m_depth(0)
    , m_tick(0)
    , m_chunkId(m_structure.nominalChunkIndex())
    , m_chunkNum(0)
    , m_pointsPerChunk(m_structure.basePointsPerChunk())
{ }

void PointState::climb(const Dir dir)
{
    if (++m_depth <= m_startDepth) return;

    const std::size_t workingDepth(m_depth - m_startDepth);

    if (m_structure.tubular() && workingDepth <= Tube::maxTickDepth())
    {
        m_tick <<= 1;
        if (isUp(dir)) ++m_tick;
    }

    m_index <<= m_structure.dimensions();
    m_index.incSimple();
    m_index += toIntegral(dir, m_structure.tubular());

    m_bbox.go(dir);

    if (workingDepth <= m_structure.nominalChunkDepth()) return;

    if (
            workingDepth <= m_structure.sparseDepthBegin() ||
            !m_structure.sparseDepthBegin())
    {
        m_chunkId <<= m_structure.dimensions();
        m_chunkId.incSimple();

        const std::size_t chunkRatio(
                (m_index - m_chunkId).getSimple() /
                m_pointsPerChunk.getSimple());

        assert(chunkRatio < m_structure.factor());

        m_chunkId += chunkRatio * m_pointsPerChunk;

        if (workingDepth >= m_structure.coldDepthBegin())
        {
            m_chunkNum =
                (m_chunkId - m_structure.coldIndexBegin()) / m_pointsPerChunk;
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






OHierarchyState::OHierarchyState(const Metadata& metadata, OHierarchy* hierarchy)
    : m_depth(0)
    , m_climber(
        hierarchy ?
            new HierarchyClimber(
                *hierarchy,
                metadata.structure().tubular() ||
                metadata.structure().dimensions() == 3 ? 3 : 2) :
            nullptr)
{ }

} // namespace entwine


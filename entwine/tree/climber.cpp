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
        const Bounds& bounds,
        const std::size_t depth)
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

void PointState::climb(const Dir dir)
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

    if (workingDepth <= m_structure.nominalChunkDepth()) return;

    if (workingDepth <= m_structure.sparseDepthBegin())
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

} // namespace entwine


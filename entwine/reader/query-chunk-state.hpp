/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>

#include <entwine/types/bounds.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class QueryChunkState
{
public:
    QueryChunkState(const Structure& structure, const Bounds& bounds)
        : m_structure(structure)
        , m_bounds(bounds)
        , m_depth(m_structure.nominalChunkDepth())
        , m_chunkId(m_structure.nominalChunkIndex())
        , m_pointsPerChunk(m_structure.basePointsPerChunk())
    { }

    bool allDirections() const
    {
        return
                m_depth + 1 <= m_structure.sparseDepthBegin() ||
                !m_structure.sparseDepthBegin();
    }

    // Call this if allDirections() == true.
    QueryChunkState getClimb(Dir dir) const
    {
        QueryChunkState result(*this);
        ++result.m_depth;
        result.m_bounds.go(dir, m_structure.tubular());

        assert(result.m_depth <= m_structure.sparseDepthBegin());

        result.m_chunkId <<= m_structure.dimensions();
        ++result.m_chunkId.data().front();
        result.m_chunkId += toIntegral(dir) * m_pointsPerChunk;

        return result;
    }

    // Else call this.
    QueryChunkState getClimb() const
    {
        QueryChunkState result(*this);
        ++result.m_depth;
        result.m_chunkId <<= m_structure.dimensions();
        ++result.m_chunkId.data().front();
        result.m_pointsPerChunk *= m_structure.factor();

        return result;
    }

    const Bounds& bounds() const { return m_bounds; }
    std::size_t depth() const { return m_depth; }
    const Id& chunkId() const { return m_chunkId; }
    const Id& pointsPerChunk() const { return m_pointsPerChunk; }

private:
    QueryChunkState(const QueryChunkState& other) = default;

    const Structure& m_structure;
    Bounds m_bounds;
    std::size_t m_depth;

    Id m_chunkId;
    Id m_pointsPerChunk;
};

} // namespace entwine


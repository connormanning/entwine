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
#include <deque>

#include <entwine/reader/cache.hpp>
#include <entwine/reader/comparison.hpp>
#include <entwine/reader/filter.hpp>
#include <entwine/reader/reader.hpp>
#include <entwine/types/dir.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/pooled-point-table.hpp>

namespace entwine
{

class PointInfo;
class PointState;
class Schema;

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

class Query
{
public:
    Query(
            const Reader& reader,
            const Schema& schema,
            const Json::Value& filter,
            Cache& cache,
            const Bounds& queryBounds,
            std::size_t depthBegin,
            std::size_t depthEnd,
            double scale,
            const Point& offset);

    // Returns true if next() should be called again.  If false is returned,
    // then the query is complete and next() should not be called anymore.
    bool next(std::vector<char>& buffer);

    bool done() const { return m_done; }
    std::size_t numPoints() const { return m_numPoints; }

protected:
    bool getBase(std::vector<char>& buffer); // True if base data existed.
    void getChunked(std::vector<char>& buffer);

    void getFetches(const QueryChunkState& chunkState);
    void getBase(std::vector<char>& buffer, const PointState& pointState);

    template<typename T> void setSpatial(char* pos, double d)
    {
        const T v(d);
        std::memcpy(pos, &v, sizeof(T));
    }

    bool processPoint(std::vector<char>& buffer, const PointInfo& info);

    const Reader& m_reader;
    const Structure& m_structure;
    Cache& m_cache;

    Bounds m_queryBounds;
    const std::size_t m_depthBegin;
    const std::size_t m_depthEnd;

    FetchInfoSet m_chunks;
    std::unique_ptr<Block> m_block;
    ChunkMap::const_iterator m_chunkReaderIt;

    std::size_t m_numPoints;

    bool m_base;
    bool m_done;

    const Schema& m_outSchema;
    const double m_scale;
    const Point m_offset;

    BinaryPointTable m_table;
    pdal::PointRef m_pointRef;

    Filter m_filter;
};

} // namespace entwine


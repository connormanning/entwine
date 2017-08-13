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

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <deque>

#include <entwine/reader/cache.hpp>
#include <entwine/reader/chunk-reader.hpp>
#include <entwine/reader/comparison.hpp>
#include <entwine/reader/filter.hpp>
#include <entwine/types/binary-point-table.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/dir.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class Cache;
class PointInfo;
class PointState;
class Reader;
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
            std::size_t depthBegin,
            std::size_t depthEnd,
            const Point* scale = nullptr,
            const Point* offset = nullptr);

    Query(
            const Reader& reader,
            const Schema& schema,
            const Json::Value& filter,
            Cache& cache,
            const Bounds& queryBounds,
            std::size_t depthBegin,
            std::size_t depthEnd,
            const Point* scale = nullptr,
            const Point* offset = nullptr);

    std::vector<char> run()
    {
        std::vector<char> buffer;
        while (!done()) next(buffer);
        return buffer;
    }

    // Returns true if next() should be called again.  If false is returned,
    // then the query is complete and next() should not be called anymore.
    bool next(std::vector<char>& buffer);

    void write(std::string name, const std::vector<char>& data);

    bool done() const { return m_done; }
    std::size_t numPoints() const { return m_numPoints; }

    const Schema& schema() const { return m_outSchema; }

protected:
    void getFetches(const QueryChunkState& chunkState);

    void getBase(std::vector<char>& buffer, const PointState& pointState);
    void getChunked(std::vector<char>& buffer);
    bool processPoint(
            std::vector<char>& buffer,
            const PointInfo& info,
            std::size_t baseDepth = 0);

    template<typename T> void setAs(char* dst, double d)
    {
        const T v(d);
        auto src(reinterpret_cast<const char*>(&v));
        std::copy(src, src + sizeof(T), dst);
    }

    const Reader& m_reader;
    const Structure& m_structure;
    Cache& m_cache;

    std::unique_ptr<Delta> m_delta;
    Bounds m_queryBounds;
    const std::size_t m_depthBegin;
    const std::size_t m_depthEnd;

    FetchInfoSet m_chunks;
    std::unique_ptr<Block> m_block;
    ChunkMap::const_iterator m_chunkReaderIt;

    std::size_t m_numPoints;

    bool m_base;
    bool m_done;

    Schema m_outSchema;

    BinaryPointTable m_table;
    pdal::PointRef m_pointRef;

    Filter m_filter;

    // Write-side stuff.

    void writeBase(const PointState& pointState);
    void writeChunked();

    std::string m_name;
    const std::vector<char>* m_data = nullptr;
    const char* m_pos = nullptr;
    BaseExtra* m_baseExtra = nullptr;
    std::unique_ptr<pdal::Dimension::Id> m_maskId;

    std::unique_ptr<BinaryPointTable> m_writeTable;
    std::unique_ptr<pdal::PointRef> m_writeRef;

    // Read-side 'extra' stuff.

    // Dim-name -> Appended set name.
    std::map<std::string, std::string> m_extraNames;
    std::map<std::string, Extra*> m_extras;
    std::map<std::string, BaseExtra*> m_baseExtras;
};

} // namespace entwine


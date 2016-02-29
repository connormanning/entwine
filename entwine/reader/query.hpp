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

#include <cstddef>
#include <deque>

#include <entwine/reader/cache.hpp>
#include <entwine/reader/reader.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/pooled-point-table.hpp>

namespace entwine
{

class Schema;

class Query
{
public:
    Query(
            const Reader& reader,
            const Schema& schema,
            Cache& cache,
            const BBox& qbox,
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

    template<typename T> void setSpatial(char* pos, double d)
    {
        const T v(d);
        std::memcpy(pos, &v, sizeof(T));
    }

    bool processPoint(
            std::vector<char>& buffer,
            const PointInfo& info);

    const Reader& m_reader;
    const Structure& m_structure;
    Cache& m_cache;

    const BBox m_qbox;
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
};

} // namespace entwine


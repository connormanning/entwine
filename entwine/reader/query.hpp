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

#include <entwine/reader/reader.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/single-point-table.hpp>

namespace entwine
{

class Schema;

class Query
{
    friend class Reader;

public:
    Query(
            const Reader& reader,
            const Structure& structure,
            const Schema& schema,
            Cache& cache,
            const BBox& qbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    // Get an arbitrary number of points selected by this query.  If the size
    // of the result is zero, the query is complete.
    void next(std::vector<char>& buffer);

    bool done() const { return m_done; }

    // Get the number of points selected by this query, only valid once the
    // query is complete.
    std::size_t numPoints() const { return m_numPoints; }

private:
    void getBase(std::vector<char>& buffer);
    void getChunked(std::vector<char>& buffer);

    const Reader& m_reader;
    const Structure& m_structure;
    const Schema& m_outSchema;

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
};

} // namespace entwine


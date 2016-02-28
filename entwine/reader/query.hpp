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

class BaseQuery
{
public:
    BaseQuery(
            const Reader& reader,
            Cache& cache,
            const BBox& qbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    virtual ~BaseQuery() { }

    // Returns true if next() should be called again.  If false is returned,
    // then the query is complete and next() should not be called anymore.
    //
    // This function is primarily for callers that consume the data during the
    // query, for example data streamers.  For callers caring only about the
    // end result (e.g. the MetaQuery), run() is simpler to use.
    bool next();

    void run() { while (next()) ; }

    bool done() const { return m_done; }

    std::size_t numPoints() const { return m_numPoints; }

protected:
    virtual bool processPoint(const PointInfo& info) = 0;

    bool getBase(); // Returns true if base data existed.
    void getChunked();

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
};

class Query : public BaseQuery
{
public:
    Query(
            const Reader& reader,
            const Schema& schema,
            Cache& cache,
            const BBox& qbox,
            std::size_t depthBegin,
            std::size_t depthEnd,
            bool normalize,
            double scale);

    bool next(std::vector<char>& buffer);

private:
    virtual bool processPoint(const PointInfo& info) override;

    std::vector<char>* m_buffer;
    const Schema& m_outSchema;
    const bool m_normalize;
    const double m_scale;

    const Point& m_readerMid;

    BinaryPointTable m_table;
    pdal::PointRef m_pointRef;
};

class MetaQuery : public BaseQuery
{
public:
    MetaQuery(
            const Reader& reader,
            Cache& cache,
            const BBox& qbox,
            Reader::BoxMap& grid,
            std::size_t depth)
        : BaseQuery(reader, cache, qbox, depth, depth + 1)
        , m_grid(grid)
        , m_loBox()
        , m_hiBox()
        , m_radius(qbox.width() / 2)    // TODO This assumes an actual cube.
        , m_is3d(qbox.is3d())
    { }

private:
    virtual bool processPoint(const PointInfo& info) override;

    Reader::BoxMap& m_grid;
    BBox m_loBox;
    BBox m_hiBox;
    const double m_radius;
    const bool m_is3d;
};

} // namespace entwine


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
#include <entwine/reader/query-chunk-state.hpp>
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

class Query
{
public:
    Query(
            const Reader& reader,
            const Bounds& bounds = Bounds::everything(),
            const Delta& delta = Delta(),
            std::size_t depthBegin = 0,
            std::size_t depthEnd = 0,
            const Json::Value& filter = Json::Value());

    virtual ~Query() { }

    bool next();
    void run() { while (!done()) next(); }

    bool done() const { return m_done; }
    std::size_t numPoints() const { return m_numPoints; }

protected:
    virtual void process(const PointInfo& info) = 0;

    void getFetches(const QueryChunkState& c);
    void getBase(const PointState& pointState);
    void getChunked();
    void maybeAcquire();
    void processPoint(const PointInfo& info);

    const Reader& m_reader;
    const Metadata& m_metadata;
    const Structure& m_structure;
    const Delta m_delta;
    const Bounds m_bounds;
    const std::size_t m_depthBegin;
    const std::size_t m_depthEnd;
    const Filter m_filter;

    BinaryPointTable m_table;
    pdal::PointRef m_pointRef;

private:
    Delta localize(const Delta& out) const;
    Bounds localize(const Bounds& bounds, const Delta& localDelta) const;

    FetchInfoSet m_chunks;
    std::unique_ptr<Block> m_block;
    ChunkMap::const_iterator m_chunkReaderIt;

    std::size_t m_numPoints = 0;
    bool m_base = true;
    bool m_done = false;
};

class CountQuery : public Query
{
public:
    template<typename... Args>
    CountQuery(Args&&... args)
        : Query(std::forward<Args>(args)...)
    { }

protected:
    virtual void process(const PointInfo& info) override { }
};

class ReadQuery : public Query
{
public:
    template<typename... Args>
    ReadQuery(
            const Reader& reader,
            const Bounds& bounds = Bounds::everything(),
            const Delta& delta = Delta(),
            std::size_t depthBegin = 0,
            std::size_t depthEnd = 0,
            const Json::Value& filter = Json::Value(),
            const Schema& schema = Schema())
        : Query(reader, bounds, delta, depthBegin, depthEnd, filter)
        , m_schema(schema.empty() ? m_metadata.schema() : schema)
        , m_mid(m_metadata.boundsScaledCubic().mid())
    { }

    const std::vector<char>& data() const { return m_data; }
    std::vector<char>& data() { return m_data; }

    const Schema& schema() const { return m_schema; }

protected:
    virtual void process(const PointInfo& info) override;

private:
    void setScaled(const DimInfo& dim, std::size_t dimNum, char* pos)
    {
        const double d = Point::scale(
                m_pointRef.getFieldAs<double>(dim.id()),
                m_mid[dimNum],
                m_delta.scale()[dimNum],
                m_delta.offset()[dimNum]);

        switch (dim.type())
        {
            case pdal::Dimension::Type::Double:     setAs<double>(pos, d);
                break;
            case pdal::Dimension::Type::Float:      setAs<float>(pos, d);
                break;
            case pdal::Dimension::Type::Unsigned8:  setAs<uint8_t>(pos, d);
                break;
            case pdal::Dimension::Type::Signed8:    setAs<int8_t>(pos, d);
                break;
            case pdal::Dimension::Type::Unsigned16: setAs<uint16_t>(pos, d);
                break;
            case pdal::Dimension::Type::Signed16:   setAs<int16_t>(pos, d);
                break;
            case pdal::Dimension::Type::Unsigned32: setAs<uint32_t>(pos, d);
                break;
            case pdal::Dimension::Type::Signed32:   setAs<int32_t>(pos, d);
                break;
            case pdal::Dimension::Type::Unsigned64: setAs<uint64_t>(pos, d);
                break;
            case pdal::Dimension::Type::Signed64:   setAs<int64_t>(pos, d);
                break;
            default:
                break;
        }
    }

    template<typename T> void setAs(char* dst, double d)
    {
        const T v(d);
        auto src(reinterpret_cast<const char*>(&v));
        std::copy(src, src + sizeof(T), dst);
    }

    const Schema m_schema;
    const Point m_mid;

    std::vector<char> m_data;
};

/*
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
    const Bounds m_queryBounds;
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
*/

} // namespace entwine


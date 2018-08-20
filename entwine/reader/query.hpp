/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <entwine/reader/query-params.hpp>

#include <entwine/reader/filter.hpp>
#include <entwine/reader/hierarchy-reader.hpp>
#include <entwine/reader/chunk-reader.hpp>
#include <entwine/types/binary-point-table.hpp>
#include <entwine/types/key.hpp>
#include <entwine/types/schema.hpp>

namespace entwine
{

class Reader;

class Query
{
public:
    Query(const Reader& reader, const Json::Value& params);
    virtual ~Query() { }

    void run();

    uint64_t numPoints() const { return m_numPoints; }

protected:
    virtual void process(const Cell& cell) { }

    const Reader& m_reader;
    const Metadata& m_metadata;
    const HierarchyReader& m_hierarchy;
    const QueryParams m_params;
    const Filter m_filter;

    BinaryPointTable m_table;
    pdal::PointRef m_pointRef;

private:
    HierarchyReader::Keys overlaps() const;
    void overlaps(HierarchyReader::Keys& keys, const ChunkKey& c) const;

    void maybeProcess(const Cell& cell);

    HierarchyReader::Keys m_overlaps;
    uint64_t m_numPoints = 0;
    std::deque<SharedChunkReader> m_chunks;
};

class CountQuery : public Query
{
public:
    CountQuery(const Reader& reader, const Json::Value& json)
        : Query(reader, json)
    { }
};

class ReadQuery : public Query
{
public:
    ReadQuery(const Reader& reader, const Json::Value& json)
        : Query(reader, json)
        , m_schema(json.isMember("schema") ?
                Schema(json["schema"]) : m_metadata.outSchema())
        , m_delta(Delta::existsIn(json) ?
                Delta(json) : Delta(m_metadata.delta()))
    { }

    const std::vector<char>& data() const { return m_data; }

protected:
    virtual void process(const Cell& cell) override;

private:
    void setAs(char* dst, double d, pdal::Dimension::Type t)
    {
        switch (t)
        {
            case pdal::Dimension::Type::Double:     setAs<double>(dst, d);
                break;
            case pdal::Dimension::Type::Float:      setAs<float>(dst, d);
                break;
            case pdal::Dimension::Type::Unsigned8:  setAs<uint8_t>(dst, d);
                break;
            case pdal::Dimension::Type::Signed8:    setAs<int8_t>(dst, d);
                break;
            case pdal::Dimension::Type::Unsigned16: setAs<uint16_t>(dst, d);
                break;
            case pdal::Dimension::Type::Signed16:   setAs<int16_t>(dst, d);
                break;
            case pdal::Dimension::Type::Unsigned32: setAs<uint32_t>(dst, d);
                break;
            case pdal::Dimension::Type::Signed32:   setAs<int32_t>(dst, d);
                break;
            case pdal::Dimension::Type::Unsigned64: setAs<uint64_t>(dst, d);
                break;
            case pdal::Dimension::Type::Signed64:   setAs<int64_t>(dst, d);
                break;
            default:
                break;
        }
    }

    template<typename T> void setAs(char* dst, double d)
    {
        const T v(static_cast<T>(d));
        auto src(reinterpret_cast<const char*>(&v));
        std::copy(src, src + sizeof(T), dst);
    }

    const Schema m_schema;
    const Delta m_delta;

    std::vector<char> m_data;
};

} // namespace entwine


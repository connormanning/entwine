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

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <entwine/third/json/json.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

enum class ChunkType : char
{
    Sparse = 0,
    Contiguous,
    Invalid
};

enum class TailField
{
    ChunkType,
    NumPoints,
    NumBytes
};

using TailFields = std::vector<TailField>;
using TailFieldLookup = std::map<TailField, std::string>;

const TailFieldLookup tailFieldNames
{
    { TailField::ChunkType, "chunkType" },
    { TailField::NumPoints, "numPoints" },
    { TailField::NumBytes, "numBytes" }
};

class Format;

class Packer
{
public:
    Packer(
            const TailFields& tailFields,
            const std::vector<char>& data,
            std::size_t numPoints,
            ChunkType chunkType)
        : m_fields(tailFields)
        , m_data(data)
        , m_numPoints(numPoints)
        , m_chunkType(chunkType)
    { }

    std::vector<char> buildTail() const;

private:
    using Data = std::vector<char>;

    Data chunkType() const
    {
        return Data { static_cast<char>(m_chunkType) };
    }

    Data numPoints() const
    {
        const char* pos(reinterpret_cast<const char*>(&m_numPoints));
        return Data(pos, pos + sizeof(uint64_t));
    }

    Data numBytes() const
    {
        const std::size_t numBytes(m_data.size());
        const char* pos(reinterpret_cast<const char*>(&numBytes));
        return Data(pos, pos + sizeof(uint64_t));
    }

    const TailFields& m_fields;
    const std::vector<char>& m_data;
    const std::size_t m_numPoints;
    const ChunkType m_chunkType;
};

class Unpacker
{
    friend class Format;
public:
    // These results are already decompressed.
    std::unique_ptr<std::vector<char>>&& acquireBytes();
    Cell::PooledStack acquireCells(PointPool& pointPool);

    // Not decompressed - just the raw data without the tail.
    std::unique_ptr<std::vector<char>>&& acquireRawBytes()
    {
        return std::move(m_data);
    }

    const ChunkType chunkType() const
    {
        if (m_chunkType) return *m_chunkType;
        else return ChunkType::Contiguous;
    }
    const std::size_t numPoints() const { return *m_numPoints; }

private:
    Unpacker(const Format& format, std::unique_ptr<std::vector<char>> data);

    void extractChunkType()
    {
        checkSize(1);
        m_chunkType = makeUnique<ChunkType>(
                static_cast<ChunkType>(m_data->back()));
        m_data->pop_back();
    }

    void extractNumPoints()
    {
        m_numPoints = makeUnique<std::size_t>(extract64());
    }

    void extractNumBytes()
    {
        m_numBytes = makeUnique<std::size_t>(extract64());
    }

    uint64_t extract64()
    {
        const std::size_t size(sizeof(uint64_t));
        checkSize(size);
        uint64_t val(0);

        const char* pos(m_data->data() + m_data->size() - size);
        std::copy(pos, pos + size, reinterpret_cast<char*>(&val));

        m_data->resize(m_data->size() - size);
        return val;
    }

    void checkSize(std::size_t minimum)
    {
        if (!m_data || m_data->size() < minimum)
        {
            throw std::runtime_error("Invalid chunk size");
        }
    }

    const Format& m_format;

    std::unique_ptr<std::vector<char>> m_data;

    std::unique_ptr<ChunkType> m_chunkType;
    std::unique_ptr<std::size_t> m_numPoints;
    std::unique_ptr<std::size_t> m_numBytes;
};

// The Format contains the attributes that give insight about what the tree
// looks like at a more micro-oriented level than the Structure, which gives
// information about the overall tree structure.  Whereas the Structure can
// tell us about the chunks that exist in the tree, the Format can tell us
// about what those chunks look like.
class Format
{
public:
    Format(
            const Schema& schema,
            bool trustHeaders,
            bool compress,
            std::vector<std::string> tailFields = std::vector<std::string> {
                "numPoints", "chunkType"
            },
            std::string srs = std::string());

    Format(const Schema& schema, const Json::Value& json);

    Json::Value toJson() const
    {
        Json::Value json;
        json["srs"] = m_srs;
        json["trustHeaders"] = m_trustHeaders;
        json["compress"] = m_compress;
        for (const TailField f : m_tailFields)
        {
            json["tail"].append(tailFieldNames.at(f));
        }
        return json;
    }

    std::unique_ptr<std::vector<char>> pack(
            Data::PooledStack dataStack,
            ChunkType chunkType) const;

    Unpacker unpack(std::unique_ptr<std::vector<char>> data) const
    {
        return Unpacker(*this, std::move(data));
    }

    const Schema& schema() const { return m_schema; }
    const TailFields& tailFields() const { return m_tailFields; }

    bool trustHeaders() const { return m_trustHeaders; }
    bool compress() const { return m_compress; }
    const std::string& srs() const { return m_srs; }
    std::string& srs() { return m_srs; }

private:
    const Schema m_schema;

    bool m_trustHeaders;
    bool m_compress;
    TailFields m_tailFields;
    std::string m_srs;
};

} // namespace entwine


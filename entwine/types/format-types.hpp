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
#include <cstddef>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include <json/json.h>

namespace entwine
{

enum class ChunkType : char { Sparse = 0, Contiguous, Invalid };
enum class TailField { ChunkType, NumPoints, NumBytes };
enum class ChunkCompression { None, LasZip, LazPerf };
enum class HierarchyCompression { None, Lzma };

using TailFieldList = std::vector<TailField>;

class Tail
{
public:
    Tail(std::vector<char>& data, TailFieldList fields)
    {
        // Fields are in reverse order as we extract.
        for (auto it(fields.rbegin()); it != fields.rend(); ++it)
        {
            switch (*it)
            {
                case TailField::ChunkType:
                    m_type = static_cast<ChunkType>(extract<char>(data));
                    break;
                case TailField::NumPoints:
                    m_numPoints = extract<uint64_t>(data);
                    break;
                case TailField::NumBytes:
                    m_numBytes = extract<uint64_t>(data);
                    break;
                default:
                    throw std::runtime_error("Invalid tail field value");
            }
        }
    }

    std::size_t size() const { return m_size; }
    ChunkType type() const { return m_type; }
    std::size_t numPoints() const { return m_numPoints; }
    std::size_t numBytes() const { return m_numBytes; }

private:
    template<typename T>
    T extract(std::vector<char>& data)
    {
        T v(0);
        const auto size(sizeof(T));
        m_size += size;

        if (data.size() < size) throw std::runtime_error("Invalid chunk size");
        const char* pos(data.data() + data.size() - size);
        std::copy(pos, pos + size, reinterpret_cast<char*>(&v));

        data.resize(data.size() - size);
        return v;
    }

    std::size_t m_size = 0;

    ChunkType m_type = ChunkType::Invalid;
    std::size_t m_numPoints = 0;
    std::size_t m_numBytes = 0;
};

inline std::string toString(ChunkCompression c)
{
    switch (c)
    {
        case ChunkCompression::LasZip: return "laszip";
        case ChunkCompression::LazPerf: return "lazperf";
        case ChunkCompression::None: return "none";
        default: throw std::runtime_error("Invalid ChunkCompression value");
    }
}

inline ChunkCompression toCompression(const Json::Value& j)
{
    if (j.isNull() || j.asString() == "none") return ChunkCompression::None;
    const std::string s(j.asString());
    if (s == "laszip") return ChunkCompression::LasZip;
    if (s == "lazperf") return ChunkCompression::LazPerf;
    throw std::runtime_error("Invalid compression: " + j.toStyledString());
}

inline std::string toString(TailField t)
{
    switch (t)
    {
        case TailField::ChunkType: return "chunkType";
        case TailField::NumPoints: return "numPoints";
        case TailField::NumBytes: return "numBytes";
        default: throw std::runtime_error("Invalid TailField value");
    }
}

inline TailField toTailField(const std::string& s)
{
    if (s == "chunkType") return TailField::ChunkType;
    if (s == "numPoints") return TailField::NumPoints;
    if (s == "numBytes") return TailField::NumBytes;
    throw std::runtime_error("Invalid tail field: " + s);
}

inline std::string toString(HierarchyCompression c)
{
    switch (c)
    {
        case HierarchyCompression::None: return "none";
        case HierarchyCompression::Lzma: return "lzma";
        default: throw std::runtime_error("Invalid HierarchyCompression value");
    }
}

inline HierarchyCompression toHierarchyCompression(const std::string& s)
{
    if (s == "lzma") return HierarchyCompression::Lzma;
    if (s == "none") return HierarchyCompression::None;
    throw std::runtime_error("Invalid hierarchy compression: " + s);
}

inline HierarchyCompression toHierarchyCompression(const Json::Value& j)
{
    return toHierarchyCompression(j.asString());
}

} // namespace entwine


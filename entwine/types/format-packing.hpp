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

#include <entwine/types/format-types.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

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

} // namespace entwine


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
#include <memory>
#include <stdexcept>
#include <vector>

#include <pdal/Compression.hpp>

#include <entwine/types/structure.hpp>

namespace entwine
{

class Schema;

class CompressionStream
{
public:
    CompressionStream(std::size_t fullBytes)
        : m_data(new std::vector<char>())
    {
        m_data->reserve(static_cast<float>(fullBytes) * 0.4);
    }

    void putBytes(const uint8_t* bytes, std::size_t length)
    {
        m_data->insert(m_data->end(), bytes, bytes + length);
    }

    void putByte(uint8_t byte)
    {
        m_data->push_back(reinterpret_cast<const char&>(byte));
    }

    std::unique_ptr<std::vector<char>> data()
    {
        std::unique_ptr<std::vector<char>> res(std::move(m_data));
        m_data.reset(new std::vector<char>());
        return res;
    }

private:
    std::unique_ptr<std::vector<char>> m_data;
};

class DecompressionStream
{
public:
    DecompressionStream(const std::vector<char>& data)
        : m_data(data)
        , m_index(0)
    { }

    uint8_t getByte()
    {
        const uint8_t val(reinterpret_cast<const uint8_t&>(m_data.at(m_index)));
        ++m_index;
        return val;
    }

    void getBytes(uint8_t* bytes, std::size_t length)
    {
        assert(m_index + length <= m_data.size());

        std::copy(
                m_data.data() + m_index,
                m_data.data() + m_index + length,
                bytes);

        m_index += length;
    }

private:
    const std::vector<char>& m_data;
    std::size_t m_index;
};

class Compression
{
public:
    static std::unique_ptr<std::vector<char>> compress(
            const std::vector<char>& data,
            const Schema& schema);

    static std::unique_ptr<std::vector<char>> compress(
            const char* data,
            std::size_t size,
            const Schema& schema);

    static std::unique_ptr<std::vector<char>> decompress(
            const std::vector<char>& data,
            const Schema& schema,
            std::size_t numPoints);

    // If wantedSchema is nullptr, then the result will be in the native schema.
    static std::unique_ptr<std::vector<char>> decompress(
            const std::vector<char>& data,
            const Schema& nativeSchema,
            const Schema* const wantedSchema,
            std::size_t numPoints);

    static Cell::PooledStack decompress(
            const std::vector<char>& data,
            std::size_t numPoints,
            PointPool& pointPool);

    static std::unique_ptr<std::vector<char>> compressLzma(
            const std::vector<char>& data);

    static std::unique_ptr<std::vector<char>> decompressLzma(
            const std::vector<char>& data);

    Compression() = delete;
};

class Compressor
{
public:
    Compressor(const Schema& schema, std::size_t numPoints = 0);
    void push(const char* data, std::size_t size);

    template<typename T>
    void push(T val)
    {
        push(reinterpret_cast<const char*>(&val), sizeof(T));
    }

    std::unique_ptr<std::vector<char>> data();

private:
    CompressionStream m_stream;
    pdal::LazPerfCompressor<CompressionStream> m_compressor;
};

} // namespace entwine



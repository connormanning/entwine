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
#include <cstdint>
#include <memory>
#include <vector>

#include <pdal/Compression.hpp>

#include <entwine/compression/stream.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class Schema;

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

    static std::unique_ptr<std::vector<char>> decompress(
            const std::vector<char>& data,
            const Schema& nativeSchema,
            const Schema& wantedSchema,
            std::size_t numPoints);

    static PooledInfoStack decompress(
            const std::vector<char>& data,
            std::size_t numPoints,
            PointPool& pointPool);
};

class Compressor
{
public:
    Compressor(const Schema& schema, std::size_t numPoints);
    void push(const char* data, std::size_t size);
    std::unique_ptr<std::vector<char>> data();

private:
    CompressionStream m_stream;
    pdal::LazPerfCompressor<CompressionStream> m_compressor;
};

} // namespace entwine


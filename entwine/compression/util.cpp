/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/compression/util.hpp>

#include <pdal/PointLayout.hpp>

#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/schema.hpp>

namespace entwine
{

std::unique_ptr<std::vector<char>> Compression::compress(
        const std::vector<char>& data,
        const Schema& schema)
{
    return compress(data.data(), data.size(), schema);
}

std::unique_ptr<std::vector<char>> Compression::compress(
        const char* data,
        const std::size_t size,
        const Schema& schema)
{
    CompressionStream compressionStream;
    pdal::LazPerfCompressor<CompressionStream> compressor(
            compressionStream,
            schema.pdalLayout().dimTypes());

    compressor.compress(data, size);
    compressor.done();

    return compressionStream.data();
}

std::unique_ptr<std::vector<char>> Compression::decompress(
        const std::vector<char>& data,
        const Schema& schema,
        const std::size_t numPoints)
{
    const std::size_t decompressedSize(numPoints * schema.pointSize());

    DecompressionStream decompressionStream(data);
    pdal::LazPerfDecompressor<DecompressionStream> decompressor(
            decompressionStream,
            schema.pdalLayout().dimTypes());

    std::unique_ptr<std::vector<char>> decompressed(
            new std::vector<char>(decompressedSize));

    decompressor.decompress(decompressed->data(), decompressed->size());

    return decompressed;
}

PooledInfoStack Compression::decompress(
        const std::vector<char>& data,
        const std::size_t numPoints,
        Pools& pools)
{
    BinaryPointTable table(pools, numPoints);
    const std::size_t pointSize(pools.schema().pointSize());

    DecompressionStream decompressionStream(data);
    pdal::LazPerfDecompressor<DecompressionStream> decompressor(
            decompressionStream,
            pools.schema().pdalLayout().dimTypes());

    for (std::size_t i(0); i < numPoints; ++i)
    {
        decompressor.decompress(table.getPoint(i), pointSize);
    }

    return table.acquire();
}

///////////////////////////////////////////////////////////////////////////////

Compressor::Compressor(const Schema& schema)
    : m_stream()
    , m_compressor(m_stream, schema.pdalLayout().dimTypes())
{ }

void Compressor::push(const char* data, const std::size_t size)
{
    m_compressor.compress(data, size);
}

std::unique_ptr<std::vector<char>> Compressor::data()
{
    m_compressor.done();
    return m_stream.data();
}

} // namespace entwine


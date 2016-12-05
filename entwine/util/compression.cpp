/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/util/compression.hpp>

#include <pdal/PointLayout.hpp>

#include <entwine/types/binary-point-table.hpp>
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
    CompressionStream compressionStream(size);
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

std::unique_ptr<std::vector<char>> Compression::decompress(
        const std::vector<char>& data,
        const Schema& nativeSchema,
        const Schema* const wantedSchema,
        const std::size_t numPoints)
{
    if (!wantedSchema || *wantedSchema == nativeSchema)
    {
        return decompress(data, nativeSchema, numPoints);
    }

    // Get decompressor in the native schema.
    DecompressionStream decompressionStream(data);
    pdal::LazPerfDecompressor<DecompressionStream> decompressor(
            decompressionStream,
            nativeSchema.pdalLayout().dimTypes());

    // Allocate room for a single point in the native schema.
    std::vector<char> nativePoint(nativeSchema.pointSize());
    BinaryPointTable table(nativeSchema, nativePoint.data());
    pdal::PointRef pointRef(table, 0);

    // Get our result space, in the desired schema, ready.
    std::unique_ptr<std::vector<char>> decompressed(
            new std::vector<char>(numPoints * wantedSchema->pointSize(), 0));
    char* pos(decompressed->data());
    const char* end(pos + decompressed->size());

    while (pos < end)
    {
        decompressor.decompress(nativePoint.data(), nativePoint.size());

        for (const auto& d : wantedSchema->dims())
        {
            pointRef.getField(pos, d.id(), d.type());
            pos += d.size();
        }
    }

    return decompressed;
}

Cell::PooledStack Compression::decompress(
        const std::vector<char>& data,
        const std::size_t numPoints,
        PointPool& pointPool)
{
    Data::PooledStack dataStack(pointPool.dataPool().acquire(numPoints));
    Cell::PooledStack cellStack(pointPool.cellPool().acquire(numPoints));

    BinaryPointTable table(pointPool.schema());
    pdal::PointRef pointRef(table, 0);

    const std::size_t pointSize(pointPool.schema().pointSize());

    DecompressionStream decompressionStream(data);
    pdal::LazPerfDecompressor<DecompressionStream> decompressor(
            decompressionStream,
            pointPool.schema().pdalLayout().dimTypes());

    for (Cell& cell : cellStack)
    {
        Data::PooledNode dataNode(dataStack.popOne());

        decompressor.decompress(*dataNode, pointSize);

        table.setPoint(*dataNode);
        cell.set(pointRef, std::move(dataNode));
    }

    return cellStack;
}

///////////////////////////////////////////////////////////////////////////////

Compressor::Compressor(const Schema& schema, std::size_t numPoints)
    : m_stream(schema.pointSize() * numPoints)
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


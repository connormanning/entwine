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
        const Schema& wantedSchema,
        const std::size_t numPoints)
{
    std::unique_ptr<std::vector<char>> decompressed(
            new std::vector<char>(numPoints * wantedSchema.pointSize(), 0));

    BinaryPointTable table(nativeSchema);
    pdal::PointRef pointRef(table, 0);
    const std::size_t nativePointSize(nativeSchema.pointSize());

    DecompressionStream decompressionStream(data);
    pdal::LazPerfDecompressor<DecompressionStream> decompressor(
            decompressionStream,
            nativeSchema.pdalLayout().dimTypes());

    char* pos(decompressed->data());
    const char* end(pos + decompressed->size());

    std::vector<char> nativePoint(nativePointSize);
    char* from(nativePoint.data());

    while (pos < end)
    {
        decompressor.decompress(from, nativePointSize);
        table.setPoint(from);

        for (const auto& d : wantedSchema.dims())
        {
            pointRef.getField(pos, d.id(), d.type());
            pos += d.size();
        }
    }

    return decompressed;
}

PooledInfoStack Compression::decompress(
        const std::vector<char>& data,
        const std::size_t numPoints,
        Pools& pools)
{
    PooledDataStack dataStack(pools.dataPool().acquire(numPoints));
    PooledInfoStack infoStack(pools.infoPool().acquire(numPoints));

    BinaryPointTable table(pools.schema());
    pdal::PointRef pointRef(table, 0);

    const std::size_t pointSize(pools.schema().pointSize());

    DecompressionStream decompressionStream(data);
    pdal::LazPerfDecompressor<DecompressionStream> decompressor(
            decompressionStream,
            pools.schema().pdalLayout().dimTypes());

    RawInfoNode* info(infoStack.head());
    char* pos(nullptr);

    while (info)
    {
        info->construct(dataStack.popOne());
        pos = info->val().data();

        decompressor.decompress(pos, pointSize);

        table.setPoint(pos);
        info->val().point(pointRef);

        info = info->next();
    }

    return infoStack;
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


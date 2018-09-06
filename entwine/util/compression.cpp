/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

/*
#include <entwine/util/compression.hpp>

#include <cassert>

#include <pdal/compression/LazPerfCompression.hpp>
#include <pdal/PointLayout.hpp>

#include <entwine/types/binary-point-table.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

std::unique_ptr<std::vector<char>> Compression::compress(
        const std::vector<char>& d,
        const Schema& schema)
{
    return Compression::compress(d.data(), d.size(), schema);
}

std::unique_ptr<std::vector<char>> Compression::compress(
        const char* data,
        const std::size_t size,
        const Schema& schema)
{
    auto v(makeUnique<std::vector<char>>());
    v->reserve(static_cast<std::size_t>(static_cast<double>(size) * 0.2));

    const auto dimTypes(schema.pdalLayout().dimTypes());
    auto cb([&v](char* p, std::size_t s)
    {
        v->insert(v->end(), p, p + s);
    });

    auto compressor(makeUnique<pdal::LazPerfCompressor>(cb, dimTypes));
    compressor->compress(data, size);
    compressor->done();

    return v;
}

std::unique_ptr<std::vector<char>> Compression::decompress(
        const std::vector<char>& data,
        const Schema& schema,
        const std::size_t numPoints)
{
    const std::size_t decompressedSize(numPoints * schema.pointSize());

    auto v(makeUnique<std::vector<char>>());
    v->reserve(decompressedSize);

    auto cb([&v](char* p, std::size_t s) { v->insert(v->end(), p, p + s); });
    const auto dimTypes(schema.pdalLayout().dimTypes());

    auto decompressor(
            makeUnique<pdal::LazPerfDecompressor>(cb, dimTypes, numPoints));
    decompressor->decompress(data.data(), data.size());
    decompressor->done();

    return v;
}

std::unique_ptr<std::vector<char>> Compression::decompress(
        const std::vector<char>& data,
        const Schema& nativeSchema,
        const Schema* const wantedSchema,
        const std::size_t np)
{
    if (!wantedSchema || *wantedSchema == nativeSchema)
    {
        return decompress(data, nativeSchema, np);
    }

    auto v(makeUnique<std::vector<char>>(np * wantedSchema->pointSize(), 0));
    char* to(v->data());

    const auto wantedDims(wantedSchema->dims());

    BinaryPointTable table(nativeSchema);
    pdal::PointRef pointRef(table, 0);

    auto cb([&to, &wantedDims, &table, &pointRef]
            (const char* from, std::size_t s)
    {
        table.setPoint(from);
        for (const auto& d : wantedDims)
        {
            pointRef.getField(to, d.id(), d.type());
            to += d.size();
        }
    });

    const auto nativeDimTypes(nativeSchema.pdalLayout().dimTypes());

    auto decompressor(
            makeUnique<pdal::LazPerfDecompressor>(cb, nativeDimTypes, np));
    decompressor->decompress(data.data(), data.size());
    decompressor->done();

    return v;
}

Cell::PooledStack Compression::decompress(
        const std::vector<char>& data,
        const std::size_t numPoints,
        PointPool& pointPool)
{
    Data::PooledStack dataStack(pointPool.dataPool().acquire(numPoints));
    Cell::PooledStack cellStack(pointPool.cellPool().acquire(numPoints));
    Cell::RawNode* current(cellStack.head());

    const auto& schema(pointPool.schema());
    BinaryPointTable table(schema);
    pdal::PointRef pointRef(table, 0);

    const auto dimTypes(schema.pdalLayout().dimTypes());

    auto cb([&dataStack, &table, &pointRef, &current]
            (const char* pos, std::size_t size)
    {
        Data::PooledNode dataNode(dataStack.popOne());

        std::copy(pos, pos + size, *dataNode);
        table.setPoint(*dataNode);

        (*current)->set(pointRef, std::move(dataNode));
        current = current->next();
    });

    auto decompressor(
            makeUnique<pdal::LazPerfDecompressor>(cb, dimTypes, numPoints));
    decompressor->decompress(data.data(), data.size());
    decompressor->done();

    assert(dataStack.empty());

    return cellStack;
}

} // namespace entwine
*/


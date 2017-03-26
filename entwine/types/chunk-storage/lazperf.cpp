/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/chunk-storage/lazperf.hpp>

namespace entwine
{

void LazPerfStorage::write(Chunk& chunk) const
{
    // TODO Use CompressionStream instead of building a single buffer.
    const auto data(buildData(chunk));
    const auto& schema(chunk.schema());
    const std::size_t numPoints(data.size() / schema.pointSize());

    auto comp(Compression::compress(data, schema));
    append(*comp, buildTail(chunk, numPoints, comp->size()));
    ensurePut(chunk, m_metadata.basename(chunk.id()), *comp);
}

Cell::PooledStack LazPerfStorage::read(
        const arbiter::Endpoint& endpoint,
        PointPool& pool,
        const Id& id) const
{
    auto compressed(io::ensureGet(endpoint, m_metadata.basename(id)));
    const Tail tail(*compressed, m_tailFields);

    const Schema& schema(pool.schema());
    const std::size_t pointSize(schema.pointSize());
    const std::size_t numPoints(tail.numPoints());
    const std::size_t numBytes(compressed->size() + tail.size());
    BinaryPointTable table(schema);
    pdal::PointRef pointRef(table, 0);

    if (id >= m_metadata.structure().coldIndexBegin() && !numPoints)
    {
        throw std::runtime_error("Invalid lazperf chunk - no numPoints");
    }
    if (tail.numBytes() && tail.numBytes() != numBytes)
    {
        std::cout << tail.numBytes() << " != " << numBytes << std::endl;
        throw std::runtime_error("Invalid lazperf chunk numBytes");
    }

    Data::PooledStack dataStack(pool.dataPool().acquire(numPoints));
    Cell::PooledStack cellStack(pool.cellPool().acquire(numPoints));

    DecompressionStream stream(*compressed);
    pdal::LazPerfDecompressor<DecompressionStream> decompressor(
            stream,
            schema.pdalLayout().dimTypes());

    for (Cell& cell : cellStack)
    {
        Data::PooledNode dataNode(dataStack.popOne());
        table.setPoint(*dataNode);
        decompressor.decompress(*dataNode, pointSize);

        cell.set(pointRef, std::move(dataNode));
    }

    assert(dataStack.empty());
    return cellStack;
}

} // namespace entwine


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
    const std::vector<char> data(buildData(chunk));
    const auto& schema(chunk.schema());

    auto comp(Compression::compress(data.data(), data.size(), schema));

    const std::size_t numPoints(data.size() / schema.pointSize());
    append(*comp, buildTail(chunk, numPoints, comp->size()));
    ensurePut(chunk, m_metadata.basename(chunk.id()), *comp);
}

Cell::PooledStack LazPerfStorage::read(
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        PointPool& pool,
        const Id& id) const
{
    auto compressed(io::ensureGet(out, m_metadata.basename(id)));
    const Tail tail(*compressed, m_tailFields);

    const std::size_t numPoints(tail.numPoints());
    const std::size_t numBytes(compressed->size() + tail.size());

    if (id >= m_metadata.structure().coldIndexBegin() && !numPoints)
    {
        throw std::runtime_error("Invalid lazperf chunk - no numPoints");
    }
    if (tail.numBytes() && tail.numBytes() != numBytes)
    {
        std::cout << tail.numBytes() << " != " << numBytes << std::endl;
        throw std::runtime_error("Invalid lazperf chunk numBytes");
    }

    return Compression::decompress(*compressed, numPoints, pool);
}

} // namespace entwine


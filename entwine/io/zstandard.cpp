/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/io/zstandard.hpp>

#include <pdal/compression/ZstdCompression.hpp>

namespace entwine
{

void Zstandard::write(
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        PointPool& pointPool,
        const std::string& filename,
        Cell::PooledStack&& cells,
        const uint64_t np) const
{
    const auto uncompressed(getBuffer(cells, np));

    std::vector<char> compressed;
    pdal::ZstdCompressor compressor([&compressed](char* pos, std::size_t size)
    {
        compressed.insert(compressed.end(), pos, pos + size);
    });

    compressor.compress(uncompressed.data(), uncompressed.size());
    compressor.done();

    writeBuffer(out, filename + ".zst", compressed);
}

Cell::PooledStack Zstandard::read(
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        PointPool& pool,
        const std::string& filename) const
{
    std::vector<char> uncompressed;
    const auto compressed(getBuffer(out, filename + ".zst"));

    pdal::ZstdDecompressor dec([&uncompressed](char* pos, std::size_t size)
    {
        uncompressed.insert(uncompressed.end(), pos, pos + size);
    });

    dec.decompress(compressed.data(), compressed.size());
    dec.done();

    return getCells(pool, uncompressed);
}

} // namespace entwine


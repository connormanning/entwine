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
        const std::string& filename,
        const Bounds& bounds,
        BlockPointTable& src) const
{
    const std::vector<char> uncompressed(pack(src));

    std::vector<char> compressed;
    pdal::ZstdCompressor compressor([&compressed](char* pos, std::size_t size)
    {
        compressed.insert(compressed.end(), pos, pos + size);
    }, 3 /* ZSTD_CLEVEL_DEFAULT */);

    compressor.compress(uncompressed.data(), uncompressed.size());
    compressor.done();

    ensurePut(out, filename + ".zst", compressed);
}

void Zstandard::read(
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        const std::string& filename,
        VectorPointTable& dst) const
{
    auto compressed(*ensureGet(out, filename + ".zst"));

    std::vector<char> uncompressed;
    pdal::ZstdDecompressor dec([&uncompressed](char* pos, std::size_t size)
    {
        uncompressed.insert(uncompressed.end(), pos, pos + size);
    });

    dec.decompress(compressed.data(), compressed.size());

    unpack(dst, std::move(uncompressed));
}

} // namespace entwine


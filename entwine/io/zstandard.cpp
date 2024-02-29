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
#ifndef NO_ZSTD
#include <pdal/compression/ZstdCompression.hpp>
#endif
#include <pdal/filters/SortFilter.hpp>

#include <entwine/io/binary.hpp>
#include <entwine/util/io.hpp>

namespace entwine
{
namespace io
{

void Zstandard::write(
    const std::string filename,
    BlockPointTable& table,
    const Bounds bounds) const
{
#ifndef NO_ZSTD
    const std::vector<char> uncompressed = binary::pack(metadata, table);

    std::vector<char> compressed;
    pdal::ZstdCompressor compressor([&compressed](char* pos, std::size_t size)
    {
        compressed.insert(compressed.end(), pos, pos + size);
    }, 3); // ZSTD_CLEVEL_DEFAULT = 3.

    compressor.compress(uncompressed.data(), uncompressed.size());
    compressor.done();

    ensurePut(endpoints.data, filename + ".zst", compressed);
#else
    throw std::runtime_error("Entwine was not built with zstd support.");
#endif
}

void Zstandard::read(std::string filename, VectorPointTable& table) const
{
#ifndef NO_ZSTD
    const std::vector<char> compressed = ensureGetBinary(
        endpoints.data,
        filename + ".zst");

    std::vector<char> uncompressed;
    pdal::ZstdDecompressor dec([&uncompressed](char* pos, std::size_t size)
    {
        uncompressed.insert(uncompressed.end(), pos, pos + size);
    });

    dec.decompress(compressed.data(), compressed.size());

    binary::unpack(metadata, table, std::move(uncompressed));
#else
    throw std::runtime_error("Entwine was not built with zstd support.");
#endif
}

} // namespace io
} // namespace entwine

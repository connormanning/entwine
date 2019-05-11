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
#include <pdal/filters/SortFilter.hpp>

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

    /*
    const Schema& outSchema(m_metadata.outSchema());
    VectorPointTable table(outSchema, pack(src));

    pdal::PointView view(table);
    for (std::size_t i(0); i < table.size(); ++i) view.getOrAddPoint(i);
    if (outSchema.hasTime())
    {
        std::sort(
                view.begin(),
                view.end(),
                [](const pdal::PointIdxRef& a, const pdal::PointIdxRef& b)
                {
                    return a.compare(DimId::GpsTime, b);
                });
    }
    */

    std::vector<char> compressed;
    pdal::ZstdCompressor compressor([&compressed](char* pos, std::size_t size)
    {
        compressed.insert(compressed.end(), pos, pos + size);
    }, 3); // ZSTD_CLEVEL_DEFAULT = 3.

    /*
    for (std::size_t i(0); i < view.size(); ++i)
    {
        compressor.compress(view.getPoint(i), outSchema.pointSize());
    }
    */
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


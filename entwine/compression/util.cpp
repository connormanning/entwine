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

#include <pdal/Compression.hpp>

#include <entwine/compression/stream.hpp>

namespace entwine
{

std::unique_ptr<std::vector<char>> Compression::compress(
        const std::vector<char>& data,
        const pdal::DimTypeList dimTypeList)
{
    CompressionStream compressionStream;
    pdal::LazPerfCompressor<CompressionStream> compressor(
            compressionStream,
            dimTypeList);

    compressor.compress(data.data(), data.size());
    compressor.done();

    std::unique_ptr<std::vector<char>> compressed(
            new std::vector<char>(compressionStream.data().size()));

    std::memcpy(
            compressed->data(),
            compressionStream.data().data(),
            compressed->size());

    return compressed;
}

std::unique_ptr<std::vector<char>> Compression::decompress(
        const std::vector<char>& data,
        const pdal::DimTypeList dimTypeList,
        const std::size_t decompressedSize)
{
    CompressionStream compressionStream(data);
    pdal::LazPerfDecompressor<CompressionStream> decompressor(
            compressionStream,
            dimTypeList);

    std::unique_ptr<std::vector<char>> decompressed(
            new std::vector<char>(decompressedSize));

    decompressor.decompress(decompressed->data(), decompressed->size());

    return decompressed;
}

} // namespace entwine


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

#include <cstdio>
#include <lzma.h>

#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{

const uint32_t mode(2);
const std::size_t blockSize(BUFSIZ);

void check(lzma_ret ret)
{
    switch (ret)
    {
        case LZMA_MEM_ERROR:
            throw std::runtime_error("Memory allocation failed");
            break;
        case LZMA_DATA_ERROR:
            throw std::runtime_error("File size limits exceeded");
            break;
        case LZMA_OPTIONS_ERROR:
            throw std::runtime_error("Unsupported preset");
            break;
        case LZMA_UNSUPPORTED_CHECK:
            throw std::runtime_error("Unsupported integrity check");
            break;
        default:
            throw std::runtime_error("LZMA error code: " + std::to_string(ret));
            break;
    }
}

std::unique_ptr<std::vector<char>> run(
        lzma_stream& stream,
        const std::vector<char>& in)
{
    auto out(makeUnique<std::vector<char>>());

    lzma_ret ret(LZMA_OK);

    const unsigned char* inStart(
            reinterpret_cast<const unsigned char*>(in.data()));

    do
    {
        out->resize(out->size() + blockSize);

        stream.next_in = inStart + stream.total_in;
        stream.avail_in = in.size() - stream.total_in;

        stream.next_out = reinterpret_cast<unsigned char*>(
                out->data() + stream.total_out);
        stream.avail_out = out->size() - stream.total_out;

        ret = lzma_code(&stream, stream.avail_in ? LZMA_RUN : LZMA_FINISH);
    }
    while (ret == LZMA_OK);

    lzma_end(&stream);

    if (ret != LZMA_STREAM_END) check(ret);

    out->resize(stream.total_out);
    return out;
}

} // unnamed namespace

std::unique_ptr<std::vector<char>> Compression::compressLzma(
        const std::vector<char>& in)
{
    lzma_stream stream = LZMA_STREAM_INIT;
    const lzma_ret ret(lzma_easy_encoder(&stream, mode, LZMA_CHECK_CRC64));

    if (ret != LZMA_OK) check(ret);

    auto out(run(stream, in));

    // Append compressed size to guard against partial downloads.
    const uint64_t outSize(out->size());
    const char* pos(reinterpret_cast<const char*>(&outSize));
    out->insert(out->end(), pos, pos + sizeof(uint64_t));

    return out;
}

std::unique_ptr<std::vector<char>> Compression::decompressLzma(
        const std::vector<char>& in)
{
    const char* endPos(in.data() + in.size());
    const char* cpos(endPos - sizeof(uint64_t));

    // Grab sizing info from tail.
    uint64_t compressedSize(0);
    std::copy(cpos, endPos, reinterpret_cast<char*>(&compressedSize));

    if (in.size() - sizeof(uint64_t) != compressedSize)
    {
        std::cout << "Size: " << in.size() << " marker: " << compressedSize <<
            std::endl;
        throw std::runtime_error("Possible LZMA partial download detected");
    }

    lzma_stream stream = LZMA_STREAM_INIT;

    const lzma_ret ret(
            lzma_auto_decoder(
                &stream,
                UINT64_MAX,
                LZMA_TELL_UNSUPPORTED_CHECK));

    if (ret != LZMA_OK) check(ret);

    auto out(run(stream, in));

    return out;
}

} // namespace entwine


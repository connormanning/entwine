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
}

std::unique_ptr<std::vector<char>> Compression::compressLzma(
        const std::vector<char>& in)
{
    lzma_stream stream(LZMA_STREAM_INIT);
    const lzma_ret initRet(lzma_easy_encoder(&stream, mode, LZMA_CHECK_CRC64));

    if (initRet != LZMA_OK)
    {
        switch (initRet)
        {
            case LZMA_MEM_ERROR:
                throw std::runtime_error("Memory allocation failed - init");
                break;
            case LZMA_OPTIONS_ERROR:
                throw std::runtime_error("Unsupported preset");
                break;
            case LZMA_UNSUPPORTED_CHECK:
                throw std::runtime_error("Unsupported integrity check");
                break;
            default:
                throw std::runtime_error("LZMA comp init error code: " +
                        std::to_string(initRet));
                break;
        }
    }

    auto out(makeUnique<std::vector<char>>(blockSize));

    lzma_ret compRet(LZMA_OK);

    stream.next_in = reinterpret_cast<const unsigned char*>(in.data());
    stream.avail_in = in.size();

    do
    {
        stream.next_out = reinterpret_cast<unsigned char*>(
                out->data() + out->size() - blockSize);
        stream.avail_out = blockSize;

        compRet = lzma_code(&stream, LZMA_FINISH);

        stream.next_in = nullptr;
        stream.avail_in = 0;

        std::size_t added(blockSize - stream.avail_out);
        out->resize(out->size() + added);
    }
    while (compRet == LZMA_OK);

    lzma_end(&stream);

    if (compRet != LZMA_STREAM_END)
    {
        switch (compRet)
        {
            case LZMA_MEM_ERROR:
                throw std::runtime_error("Memory allocation failed - comp");
                break;
            case LZMA_DATA_ERROR:
                throw std::runtime_error("File size limits exceeded");
                break;
            default:
                throw std::runtime_error("LZMA comp error code: " +
                        std::to_string(compRet));
                break;
        }
    }

    const uint64_t outSize(stream.total_out);
    assert(stream.total_out == out->size() - blockSize);
    out->resize(outSize);

    // Append compressed size to guard against partial downloads.
    out->insert(
            out->end(),
            reinterpret_cast<const char*>(&outSize),
            reinterpret_cast<const char*>(&outSize) + sizeof(uint64_t));

    return out;
}

std::unique_ptr<std::vector<char>> Compression::decompressLzma(
        const std::vector<char>& in)
{
    const char* endPos(in.data() + in.size());
    const char* cpos(endPos - sizeof(uint64_t));

    // Grab sizing info from tail.
    uint64_t compressedSize(*reinterpret_cast<const uint64_t*>(cpos));

    if (in.size() - sizeof(uint64_t) != compressedSize)
    {
        std::cout << "Size: " << in.size() << " marker: " << compressedSize <<
            std::endl;
        throw std::runtime_error("Possible LZMA partial download detected");
    }

    lzma_stream stream(LZMA_STREAM_INIT);

    const lzma_ret initRet(
            lzma_auto_decoder(
                &stream,
                UINT64_MAX,
                LZMA_TELL_UNSUPPORTED_CHECK));

    if (initRet != LZMA_OK)
    {
        switch (initRet)
        {
            case LZMA_MEM_ERROR:
                throw std::runtime_error("Memory allocation failed - init");
                break;
            case LZMA_OPTIONS_ERROR:
                throw std::runtime_error("Unsupported preset");
                break;
            default:
                throw std::runtime_error("LZMA decomp init error code: " +
                        std::to_string(initRet));
                break;
        }
    }

    lzma_ret dcmpRet(LZMA_OK);

    // Assume the output will be at least as large as the input bumped up to
    // the nearest blockSize.
    const std::size_t initSize(
            (compressedSize / blockSize + 1) * blockSize);
    auto out(makeUnique<std::vector<char>>(initSize));
    std::size_t outOffset(0);

    const unsigned char* inStart(
            reinterpret_cast<const unsigned char*>(in.data()));

    do
    {
        stream.next_in = inStart + stream.total_in;
        stream.avail_in = in.size() - stream.total_in;

        stream.next_out = reinterpret_cast<unsigned char*>(
                out->data() + outOffset);
        stream.avail_out = out->size() - outOffset;

        dcmpRet = lzma_code(&stream, LZMA_RUN);

        if (dcmpRet == LZMA_OK)
        {
            const std::size_t added(out->size() - outOffset - stream.avail_out);
            outOffset += added;

            out->resize(out->size() + blockSize);
        }
    }
    while (dcmpRet == LZMA_OK);

    lzma_end(&stream);

    const uint64_t outSize(stream.total_out);
    assert(stream.total_out == out->size() - blockSize);
    out->resize(outSize);

    if (dcmpRet != LZMA_STREAM_END)
    {
        switch (dcmpRet)
        {
            case LZMA_MEM_ERROR:
                throw std::runtime_error("Memory allocation failed - decomp");
                break;
            case LZMA_DATA_ERROR:
                throw std::runtime_error("File size limits exceeded");
                break;
            default:
                throw std::runtime_error("LZMA decomp error code: " +
                        std::to_string(dcmpRet));
                break;
        }
    }

    return out;
}

} // namespace entwine


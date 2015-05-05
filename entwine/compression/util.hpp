/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace entwine
{

class Schema;

class Compression
{
public:
    static std::unique_ptr<std::vector<char>> compress(
            const std::vector<char>& data,
            const Schema& schema);

    static std::unique_ptr<std::vector<char>> compress(
            const char* data,
            std::size_t size,
            const Schema& schema);

    static std::unique_ptr<std::vector<char>> decompress(
            const std::vector<char>& data,
            const Schema& schema,
            std::size_t decompressedSize);

    // Append/remove the uncompressed size marker from this data chunk.
    static void pushSize(std::vector<char>& data, uint64_t size);
    static uint64_t popSize(std::vector<char>& data);
};

} // namespace entwine


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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

#include <entwine/types/point-pool.hpp>

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
            std::size_t numPoints);

    // If wantedSchema is nullptr, then the result will be in the native schema.
    static std::unique_ptr<std::vector<char>> decompress(
            const std::vector<char>& data,
            const Schema& nativeSchema,
            const Schema* const wantedSchema,
            std::size_t numPoints);

    static Cell::PooledStack decompress(
            const std::vector<char>& data,
            std::size_t numPoints,
            PointPool& pointPool);

    static std::unique_ptr<std::vector<char>> compressLzma(
            const std::vector<char>& data);

    static std::unique_ptr<std::vector<char>> decompressLzma(
            const std::vector<char>& data);

    Compression() = delete;
};

} // namespace entwine


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

#include <memory>
#include <vector>

#include <pdal/PointContext.hpp>

class Compression
{
public:
    static std::unique_ptr<std::vector<char>> compress(
            const std::vector<char>& data,
            pdal::DimTypeList dimTypeList);

    static std::unique_ptr<std::vector<char>> decompress(
            const std::vector<char>& data,
            pdal::DimTypeList dimTypeList,
            std::size_t decompressedSize);
};


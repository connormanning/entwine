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
#include <map>
#include <string>
#include <vector>

namespace entwine
{

enum class ChunkType : char
{
    Sparse = 0,
    Contiguous,
    Invalid
};

enum class TailField
{
    ChunkType,
    NumPoints,
    NumBytes
};

using TailFields = std::vector<TailField>;
using TailFieldLookup = std::map<TailField, std::string>;

const TailFieldLookup tailFieldNames
{
    { TailField::ChunkType, "chunkType" },
    { TailField::NumPoints, "numPoints" },
    { TailField::NumBytes, "numBytes" }
};

inline TailField tailFieldFromName(std::string name)
{
    const auto it(
            std::find_if(
                tailFieldNames.begin(),
                tailFieldNames.end(),
                [&name](const TailFieldLookup::value_type& p)
                {
                    return p.second == name;
                }));

    if (it == tailFieldNames.end())
    {
        throw std::runtime_error("Invalid tail field name: " + name);
    }

    return it->first;
}

enum class HierarchyCompression { None, Lzma };

using HierarchyCompressionLookup = std::map<HierarchyCompression, std::string>;
const HierarchyCompressionLookup hierarchyCompressionNames
{
    { HierarchyCompression::None, "none" },
    { HierarchyCompression::Lzma, "lzma" }
};

inline HierarchyCompression hierarchyCompressionFromName(std::string name)
{
    if (name.empty()) return HierarchyCompression::None;

    const auto it(
            std::find_if(
                hierarchyCompressionNames.begin(),
                hierarchyCompressionNames.end(),
                [&name](const HierarchyCompressionLookup::value_type& p)
                {
                    return p.second == name;
                }));

    if (it == hierarchyCompressionNames.end())
    {
        throw std::runtime_error("Invalid hierarchy compression name: " + name);
    }

    return it->first;
}

} // namespace entwine


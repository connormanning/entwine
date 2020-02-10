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
#include <string>
#include <vector>

#include <entwine/io/io.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/build-parameters.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/dimension.hpp>
#include <entwine/types/srs.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/types/version.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

struct Metadata
{
    Metadata(
        Version eptVersion,
        Schema schema,
        Bounds boundsConforming,
        Bounds boundsCubic,
        optional<Srs> srs,
        optional<Subset> subset,
        io::Type dataType,
        uint64_t span,
        BuildParameters internal);

    Version eptVersion;

    Schema schema;
    Schema absoluteSchema;
    Bounds boundsConforming;
    Bounds bounds;

    optional<Srs> srs;
    optional<Subset> subset;

    io::Type dataType = io::Type::Laszip;
    uint64_t span = 0;

    BuildParameters internal;
};

void to_json(json& j, const Metadata& m);

Bounds cubeify(Bounds bounds);

inline uint64_t getStartDepth(const Metadata& m)
{
    return std::log2(m.span);
}
inline uint64_t getSharedDepth(const Metadata& m)
{
    return m.subset ? getSplits(*m.subset) : 0;
}

inline std::string getPostfix(const Metadata& m)
{
    return m.subset ? "-" + std::to_string(m.subset->id) : "";
}
inline std::string getPostfix(const Metadata& m, uint64_t depth)
{
    return depth < getSharedDepth(m) ? getPostfix(m) : "";
}

inline bool isPrimary(const Metadata& m)
{
    return !m.subset || isPrimary(*m.subset);
}

} // namespace entwine

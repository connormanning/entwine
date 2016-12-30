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

#include <functional>
#include <string>
#include <vector>

#include <entwine/third/bigint/little-big-int.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/version.hpp>

namespace pdal { class PointView; }

namespace entwine
{

inline Version currentVersion()
{
    return Version(0, 0, 1);
}

using Id = BigUint;

using Origin = uint64_t;
static constexpr Origin invalidOrigin = std::numeric_limits<Origin>::max();

using TileFunction = std::function<void(pdal::PointView& view, Bounds bounds)>;

using Offset = Point;
using Scale = Point;

using Paths = std::vector<std::string>;

} // namespace entwine


/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstdint>
#include <map>
#include <string>

#include <pdal/filters/StatsFilter.hpp>

#include <entwine/util/json.hpp>
#include <entwine/util/optional.hpp>

namespace entwine
{

struct DimensionStats
{
    DimensionStats() = default;
    DimensionStats(const pdal::stats::Summary& s);
    explicit DimensionStats(const json& j);

    double minimum = 0.0;
    double maximum = 0.0;
    double mean = 0.0;
    double variance = 0.0;
    uint64_t count = 0;

    using Values = std::map<double, uint64_t>;
    Values values;
};

void to_json(json& j, const DimensionStats& stats);
void from_json(const json& j, DimensionStats& stats);

DimensionStats combine(DimensionStats agg, const DimensionStats& cur);

} // namespace entwine

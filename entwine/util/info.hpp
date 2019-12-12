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

#include <cmath>
#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <pdal/filters/StatsFilter.hpp>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/srs.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

class Reprojection;

struct DimensionStats
{
    DimensionStats() = default;
    DimensionStats(const pdal::stats::Summary& s)
        : minimum(s.minimum())
        , maximum(s.maximum())
        , mean(s.average())
        , variance(s.variance())
        , count(s.count())
    {
        for (const auto& bucket : s.values())
        {
            values[bucket.first] = bucket.second;
        }
    }

    double minimum = 0.0;
    double maximum = 0.0;
    double mean = 0.0;
    double variance = 0.0;
    uint64_t count = 0;
    std::map<double, uint64_t> values;
};

inline void to_json(json& j, const DimensionStats& stats)
{
    j = {
        { "minimum", getTypedValue(stats.minimum) },
        { "maximum", getTypedValue(stats.maximum) },
        { "mean", getTypedValue(stats.mean) },
        { "variance", getTypedValue(stats.variance) },
        { "stddev", getTypedValue(std::sqrt(stats.variance)) },
        { "count", stats.count }
    };

    for (const auto& p : stats.values)
    {
        j["values"].push_back({
            { "value", getTypedValue(p.first) },
            { "count", p.second }
        });
    }
}

struct DimensionDetail
{
    DimensionDetail() = default;
    DimensionDetail(
        std::string name,
        DimType type,
        const DimensionStats* stats = nullptr
    )
        : name(name)
        , type(type)
        , stats(stats ? std::make_shared<DimensionStats>(*stats) : nullptr)
    { }

    std::string name;
    DimType type = DimType::None;
    double scale = 1.0;
    double offset = 0.0;
    std::shared_ptr<DimensionStats> stats;
};

std::string dimensionType(DimType type)
{
    switch (pdal::Dimension::base(type))
    {
        case pdal::Dimension::BaseType::Signed:   return "signed";
        case pdal::Dimension::BaseType::Unsigned: return "unsigned";
        case pdal::Dimension::BaseType::Floating: return "float";
        default: return "unknown";
    }
}
std::size_t dimensionSize(DimType type) { return pdal::Dimension::size(type); }

inline void to_json(json& j, const DimensionDetail& dim)
{
    j = {
        { "name", dim.name },
        { "type", dimensionType(dim.type) },
        { "size", dimensionSize(dim.type) },
    };

    if (dim.scale != 1.0 || dim.offset != 0.0)
    {
        j.update({
            { "scale", dim.scale },
            { "offset", getTypedValue(dim.offset) }
        });
    }

    // if (dim.stats) j.update({ { "stats", *dim.stats } });
    if (dim.stats) j.update(*dim.stats);
}

using StringList = std::vector<std::string>;
using DimensionList = std::vector<DimensionDetail>;
struct PointCloudInfo
{
    StringList errors;
    StringList warnings;

    json metadata;
    Srs srs;
    Bounds bounds = Bounds::expander();
    uint64_t points = 0;
    DimensionList schema;
};

inline void to_json(json& j, const PointCloudInfo& info)
{
    j = json::object();

    if (info.warnings.size())
    {
        j["warnings"] = info.warnings;
    }
    if (info.errors.size())
    {
        j["errors"] = info.errors;
        return;
    }

    j.update({
        { "srs", info.srs },
        { "bounds", info.bounds },
        { "points", info.points },
        { "schema", info.schema },
    });

    if (!info.metadata.is_null()) j.update({ { "metadata", info.metadata } });
}

struct SourceInfo
{
    SourceInfo(std::string path) : path(path) { }
    std::string path;
    PointCloudInfo info;
};
using SourceInfoList = std::vector<SourceInfo>;

inline void to_json(json& j, const SourceInfo& source)
{
    j = source.info;
    j.update({ { "path", source.path } });
}

PointCloudInfo combine(const PointCloudInfo& a, const PointCloudInfo& b);
PointCloudInfo reduce(const SourceInfoList& infoList);

json createInfoPipeline(
    json pipeline = json::array({ json::object() }),
    const Reprojection* reprojection = nullptr);

json extractInfoPipelineFromConfig(json config);

SourceInfoList analyze(
    const json& pipeline,
    const StringList& inputs,
    unsigned int threads = 8);

SourceInfoList analyze(const json& config);

void serialize(
    const SourceInfoList& sources,
    const arbiter::Endpoint& ep,
    unsigned int threads = 8);

} // namespace entwine

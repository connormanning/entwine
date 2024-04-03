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
#include <vector>

#include <entwine/types/defs.hpp>
#include <entwine/types/dimension-stats.hpp>
#include <entwine/types/fixed-point-layout.hpp>
#include <entwine/types/scale-offset.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/optional.hpp>

namespace pdal
{
namespace Dimension
{

void from_json(const entwine::json& j, Type& t);

} // namespace Dimension
} // namespace pdal

namespace entwine
{

using Type = pdal::Dimension::Type;
std::string typeString(Type type);

struct Dimension
{
    Dimension() = default;
    Dimension(std::string name, double scale = 1, double offset = 0);
    Dimension(std::string name, Type type, double scale = 1, double offset = 0);
    Dimension(
        std::string name,
        Type type,
        optional<DimensionStats> stats,
        double scale = 1,
        double offset = 0);

    std::string name;
    Type type = Type::None;
    double scale = 1.0;
    double offset = 0.0;
    optional<DimensionStats> stats;
};

void to_json(json& j, const Dimension& dim);
void from_json(const json& j, Dimension& dim);

using Schema = std::vector<Dimension>;

uint64_t getPointSize(const Schema& dims);

const Dimension* maybeFind(const Schema& dims, std::string name);
const Dimension& find(const Schema& dims, std::string name);
Dimension* maybeFind(Schema& dims, std::string name);
Dimension& find(Schema& dims, std::string name);
bool contains(const Schema& dims, std::string name);
Schema omit(Schema dims, std::string name);
Schema omit(Schema dims, const StringList& names);
bool hasStats(const Schema& dims);
Schema clearStats(Schema dims);

Dimension combine(Dimension agg, const Dimension& dim);
Schema combine(Schema agg, const Schema& list, bool fixed = false);

Schema makeAbsolute(Schema list);
Schema fromLayout(const pdal::PointLayout& layout, bool islas);
FixedPointLayout toLayout(const Schema& list, bool islas);

Schema setScaleOffset(Schema dims, ScaleOffset so);
optional<ScaleOffset> getScaleOffset(const Schema& dims);

} // namespace entwine

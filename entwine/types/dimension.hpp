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

#include <entwine/types/dimension-stats.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/optional.hpp>

namespace pdal
{
namespace Dimension
{

inline void from_json(const entwine::json& j, Type t);

} // namespace Dimension
} // namespace pdal

namespace entwine
{
namespace dimension
{

using Type = pdal::Dimension::Type;
std::string typeString(Type type);

struct Dimension
{
    Dimension() = default;
    Dimension(std::string name, Type type);
    Dimension(std::string name, Type type, Stats stats = { });
    Dimension(const json& j);

    std::string name;
    Type type = Type::None;
    double scale = 1.0;
    double offset = 0.0;
    optional<Stats> stats;
};

void to_json(json& j, const Dimension& dim);
void from_json(const json& j, Dimension& dim);

using List = std::vector<Dimension>;

const Dimension* maybeFind(const List& dims, std::string name);
const Dimension& find(const List& dims, std::string name);
Dimension* maybeFind(List& dims, std::string name);
Dimension& find(List& dims, std::string name);

Dimension combine(Dimension agg, const Dimension& dim);
List combine(List agg, const List& list);

} // namespace dimension
} // namespace entwine

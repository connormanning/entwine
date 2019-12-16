/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/source.hpp>

namespace entwine
{
namespace source
{

Info::Info(const json& j)
    : errors(extractList<std::string>(j.value("errors", json())))
    , warnings(extractList<std::string>(j.value("warnings", json())))
    , metadata(j.value("metadata", json()))
    , srs(j.value("srs", json()))
    , bounds(j.value("bounds", json()))
    , points(j.value("points", 0))
    , dimensions(
        extractList<dimension::Dimension>(j.value("dimensions", json())))
{ }

void to_json(json& j, const Info& info)
{
    j = json::object();

    if (info.warnings.size())
    {
        j["warnings"] = info.warnings;
    }
    if (info.errors.size())
    {
        j["errors"] = info.errors;
    }

    j.update({
        { "srs", info.srs },
        { "bounds", info.bounds },
        { "points", info.points },
        { "dimensions", info.dimensions },
    });

    if (!info.metadata.is_null()) j.update({ { "metadata", info.metadata } });
}

Info combine(Info agg, const Info& cur)
{
    agg.errors.insert(
        agg.errors.end(),
        cur.errors.begin(),
        cur.errors.end());
    agg.warnings.insert(
        agg.warnings.end(),
        cur.warnings.begin(),
        cur.warnings.end());

    agg.metadata = json();
    if (agg.srs.empty()) agg.srs = cur.srs;
    agg.bounds.grow(cur.bounds);
    agg.points += cur.points;
    agg.dimensions = dimension::combine(agg.dimensions, cur.dimensions);

    return agg;
}

Info combine(Info agg, Source source)
{
    for (auto& w : source.info.warnings) w = source.path + ": " + w;
    for (auto& e : source.info.errors) e = source.path + ": " + e;
    return combine(agg, source.info);
}

Info reduce(const List& list)
{
    Info initial;
    initial.bounds = Bounds::expander();
    return std::accumulate(
        list.begin(),
        list.end(),
        initial,
        [](Info info, Source source) { return combine(info, source); }
    );
}

} // namespace source
} // namespace entwine

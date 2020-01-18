/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/io/io.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/srs.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Metadata::Metadata(
    Version eptVersion,
    Schema schema,
    Bounds boundsConforming,
    Bounds bounds,
    optional<Srs> srs,
    optional<Subset> subset,
    io::Type dataType,
    uint64_t span,
    BuildParameters internal)
    : eptVersion(eptVersion)
    , schema(schema)
    , absoluteSchema(makeAbsolute(schema))
    , boundsConforming(boundsConforming)
    , bounds(bounds)
    , srs(srs)
    , subset(subset)
    , dataType(dataType)
    , span(span)
    , internal(internal)
{ }

void to_json(json& j, const Metadata& m)
{
    const json bounds = json::array({
        getTypedValue(m.bounds[0]),
        getTypedValue(m.bounds[1]),
        getTypedValue(m.bounds[2]),
        getTypedValue(m.bounds[3]),
        getTypedValue(m.bounds[4]),
        getTypedValue(m.bounds[5]),
    });

    j = {
        { "version", m.eptVersion },
        { "bounds", bounds },
        { "boundsConforming", m.boundsConforming },
        { "schema", m.schema },
        { "span", m.span },
        { "dataType", m.dataType },
        { "hierarchyType", "json" }
    };

    if (m.srs) j.update({ { "srs", *m.srs } });
    if (m.subset) j.update({ { "subset", *m.subset } });
}

Bounds cubeify(Bounds b)
{
    double diam(std::max(std::max(b.width(), b.depth()), b.height()));
    double r(std::ceil(diam / 2.0) + 1.0);

    const Point mid(b.mid().apply([](double d) { return std::round(d); }));
    return Bounds(mid - r, mid + r);
}

} // namespace entwine

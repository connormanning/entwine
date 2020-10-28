/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/dimension.hpp>

#include <entwine/types/defs.hpp>

namespace pdal
{
namespace Dimension
{

inline void from_json(const entwine::json& j, Type& t)
{
    const std::string type(j.at("type").get<std::string>());
    const int size(j.at("size").get<int>());
    t = Type::None;
    if (type == "unsigned")
    {
        if (size == 1) t = Type::Unsigned8;
        else if (size == 2) t = Type::Unsigned16;
        else if (size == 4) t = Type::Unsigned32;
        else if (size == 8) t = Type::Unsigned64;
    }
    else if (type == "signed")
    {
        if (size == 1) t = Type::Signed8;
        else if (size == 2) t = Type::Signed16;
        else if (size == 4) t = Type::Signed32;
        else if (size == 8) t = Type::Signed64;
    }
    else if (type == "float" || type == "floating")
    {
        if (size == 4) t = Type::Float;
        else if (size == 8) t = Type::Double;
    }

    if (t == Type::None)
    {
        throw std::runtime_error(
            "Invalid dimension specification: " + j.dump(2));
    }
}

} // namespace Dimension
} // namespace pdal

namespace entwine
{

Dimension::Dimension(std::string name, double scale, double offset)
    : Dimension(
        name,
        pdal::Dimension::defaultType(pdal::Dimension::id(name)),
        scale,
        offset)
{ }

Dimension::Dimension(std::string name, Type type, double scale, double offset)
    : name(name)
    , type(type)
    , scale(scale)
    , offset(offset)
{ }

Dimension::Dimension(
    std::string name,
    Type type,
    optional<DimensionStats> stats,
    double scale,
    double offset)
    : name(name)
    , type(type)
    , scale(scale)
    , offset(offset)
    , stats(stats)
{ }

std::string typeString(Type type)
{
    switch (pdal::Dimension::base(type))
    {
        case pdal::Dimension::BaseType::Signed:   return "signed";
        case pdal::Dimension::BaseType::Unsigned: return "unsigned";
        case pdal::Dimension::BaseType::Floating: return "float";
        default: return "unknown";
    }
}

void to_json(json& j, const Dimension& dim)
{
    j = {
        { "name", dim.name },
        { "type", typeString(dim.type) },
        { "size", size(dim.type) },
    };

    if (dim.scale != 1) j.update({ { "scale", dim.scale } });
    if (dim.offset != 0) j.update({ { "offset", getTypedValue(dim.offset) } });
    if (dim.stats) j.update(*dim.stats);
}

void from_json(const json& j, Dimension& dim)
{
    dim.name = j.at("name").get<std::string>();
    dim.type = j.get<Type>();
    dim.scale = j.value<double>("scale", 1);
    dim.offset = j.value<double>("offset", 0);
    if (j.count("count")) dim.stats = j.get<DimensionStats>();
}

uint64_t getPointSize(const Schema& dims)
{
    return std::accumulate(
        dims.begin(),
        dims.end(),
        0,
        [](uint64_t acc, const Dimension& d) { return acc + size(d.type); }
    );
}

const Dimension* maybeFind(const Schema& dims, std::string name)
{
    const auto it = std::find_if(
        dims.begin(),
        dims.end(),
        [name](const Dimension& dim) { return dim.name == name; }
    );
    if (it == dims.end()) return nullptr;
    return &*it;
}
Dimension* maybeFind(Schema& dims, std::string name)
{
    return const_cast<Dimension*>(
        maybeFind(static_cast<const Schema&>(dims), name)
    );
}
const Dimension& find(const Schema& dims, std::string name)
{
    if (const auto d = maybeFind(dims, name)) return *d;
    throw std::runtime_error("Failed to find dimension: " + name);
}
Dimension& find(Schema& dims, std::string name)
{
    return const_cast<Dimension&>(find(static_cast<const Schema&>(dims), name));
}
bool contains(const Schema& dims, const std::string name)
{
    return static_cast<bool>(maybeFind(dims, name));
}
Schema omit(Schema dims, const std::string name)
{
    dims.erase(
        std::remove_if(
            dims.begin(),
            dims.end(),
            [name](const Dimension& d) { return d.name == name; }));
    return dims;
}

Schema omit(Schema dims, const StringList& names)
{
    for (const auto& name : names) dims = omit(dims, name);
    return dims;
}

bool hasStats(const Schema& dims)
{
    return std::all_of(
        dims.begin(),
        dims.end(),
        [](const Dimension& d) { return static_cast<bool>(d.stats); });
}

Schema clearStats(Schema dims)
{
    for (auto& dim : dims) dim.stats = { };
    return dims;
}

Dimension combine(Dimension agg, const Dimension& dim)
{
    assert(agg.name == dim.name);
    if (size(dim.type) > size(agg.type)) agg.type = dim.type;
    agg.scale = std::min(agg.scale, dim.scale);
    // If all offsets are identical we can preserve the offset, otherwise an
    // aggregated offset is meaningless.
    if (agg.offset != dim.offset) agg.offset = 0;

    if (!agg.stats) agg.stats = dim.stats;
    else if (dim.stats) agg.stats = combine(*agg.stats, *dim.stats);

    return agg;
}

Schema makeAbsolute(Schema list)
{
    std::array<Dimension*, 3> xyz = { {
        &find(list, "X"),
        &find(list, "Y"),
        &find(list, "Z")
    } };
    for (auto d : xyz) *d = Dimension(d->name, DimType::Double, d->stats);

    return list;
}

Schema combine(Schema agg, const Schema& cur, const bool fixed)
{
    for (const auto& incoming : cur)
    {
        if (Dimension* current = maybeFind(agg, incoming.name))
        {
            *current = combine(*current, incoming);
        }
        else if (!fixed)
        {
            agg.push_back(incoming);
        }
    }
    return agg;
}

Schema fromLayout(const pdal::PointLayout& layout)
{
    Schema list;
    for (const DimId id : layout.dims())
    {
        list.push_back(Dimension(layout.dimName(id), layout.dimType(id)));
    }
    return list;
}

FixedPointLayout toLayout(const Schema& list)
{
    FixedPointLayout layout;
    for (const auto& dim : list)
    {
        layout.registerOrAssignFixedDim(dim.name, dim.type);
    }
    layout.finalize();
    return layout;
}

Schema setScaleOffset(Schema dims, const ScaleOffset so)
{
    auto& x = find(dims, "X");
    auto& y = find(dims, "Y");
    auto& z = find(dims, "Z");

    x.scale = so.scale[0];
    x.offset = so.offset[0];

    y.scale = so.scale[1];
    y.offset = so.offset[1];

    z.scale = so.scale[2];
    z.offset = so.offset[2];

    x.type = DimType::Signed32;
    y.type = DimType::Signed32;
    z.type = DimType::Signed32;

    return dims;
}

optional<ScaleOffset> getScaleOffset(const Schema& dims)
{
    const auto& x = find(dims, "X");
    const auto& y = find(dims, "Y");
    const auto& z = find(dims, "Z");
    const Scale scale(x.scale, y.scale, z.scale);
    const Offset offset(x.offset, y.offset, z.offset);

    if (scale == 1.0 && offset == 0.0) return { };
    return ScaleOffset(scale, offset);
}

} // namespace entwine

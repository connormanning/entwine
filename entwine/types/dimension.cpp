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
namespace dimension
{

Dimension::Dimension(std::string name, Type type)
    : name(name)
    , type(type)
{ }
Dimension::Dimension(std::string name, Type type, Stats stats)
    : name(name)
    , type(type)
    , stats(stats)
{ }
Dimension::Dimension(const json& j)
    : name(j.at("name").get<std::string>())
    , type(j.get<Type>())
    , scale(j.value<double>("scale", 1))
    , offset(j.value<double>("offset", 0))
{
    if (j.count("count")) stats = j.get<Stats>();
}

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

    if (dim.scale != 1.0 || dim.offset != 0.0)
    {
        j.update({
            { "scale", dim.scale },
            { "offset", getTypedValue(dim.offset) }
        });
    }

    if (dim.stats) j.update(*dim.stats);
}

void from_json(const json& j, Dimension& dim) { dim = Dimension(j); }

const Dimension* maybeFind(const List& dims, std::string name)
{
    const auto it = std::find_if(
        dims.begin(),
        dims.end(),
        [name](const Dimension& dim) { return dim.name == name; }
    );
    if (it == dims.end()) return nullptr;
    return &*it;
}
Dimension* maybeFind(List& dims, std::string name)
{
    return const_cast<Dimension*>(
        maybeFind(static_cast<const List&>(dims), name)
    );
}
const Dimension& find(const List& dims, std::string name)
{
    if (const auto d = maybeFind(dims, name)) return *d;
    throw std::runtime_error("Failed to find dimension: " + name);
}
Dimension& find(List& dims, std::string name)
{
    return const_cast<Dimension&>(find(static_cast<const List&>(dims), name));
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

List combine(List agg, const List& cur)
{
    for (const auto& incoming : cur)
    {
        Dimension* current(maybeFind(agg, incoming.name));
        if (!current) agg.push_back(incoming);
        else *current = combine(*current, incoming);
    }
    return agg;
}

} // namespace dimension
} // namespace entwine

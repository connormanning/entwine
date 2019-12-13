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

#include <entwine/types/bounds.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/dimension.hpp>
#include <entwine/types/srs.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{
namespace source
{

struct Info
{
    Info() = default;
    Info(const json& j);

    StringList errors;
    StringList warnings;

    json metadata;
    Srs srs;
    Bounds bounds = Bounds::expander();
    uint64_t points = 0;
    dimension::List dimensions;
};

using InfoList = std::vector<Info>;

void to_json(json& j, const Info& info);
inline void from_json(const json& j, Info& info) { info = Info(j); }

Info combine(Info agg, const Info& info);

struct Source
{
    Source() = default;
    Source(std::string path) : path(path) { }
    Source(const json& j)
        : path(j.at("path").get<std::string>())
        , info(j)
    { }

    std::string path;
    Info info;
};

using List = std::vector<Source>;

inline void to_json(json& j, const Source& source)
{
    j = { { "path", source.path } };
    j.update(source.info);
}
inline void from_json(const json& j, Source& source)
{
    source = Source(j);
}

Info combine(Info agg, Source source);
Info reduce(const List& list);

} // namespace source
} // namespace entwine

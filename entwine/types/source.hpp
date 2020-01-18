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

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/dimension.hpp>
#include <entwine/types/srs.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

struct SourceInfo
{
    SourceInfo() = default;
    SourceInfo(const json& j);

    StringList errors;
    StringList warnings;

    json pipeline;
    Srs srs;
    Bounds bounds;
    uint64_t points = 0;
    Schema schema;

    json metadata;
};
using InfoList = std::vector<SourceInfo>;
void to_json(json& j, const SourceInfo& info);
inline void from_json(const json& j, SourceInfo& info) { info = SourceInfo(j); }

struct Source
{
    Source() = default;
    Source(std::string path) : path(path) { }
    Source(const json& j)
        : path(j.at("path").get<std::string>())
        , info(j.get<SourceInfo>())
    { }

    std::string path;
    SourceInfo info;
};
using SourceList = std::vector<Source>;
void to_json(json& j, const Source& source);
inline void from_json(const json& j, Source& source) { source = Source(j); }

struct BuildItem
{
    BuildItem() = default;
    BuildItem(
        Source source,
        bool inserted = false,
        std::string metadataPath = "")
        : source(source)
        , inserted(inserted)
        , metadataPath(metadataPath)
    { }
    BuildItem(const json& j)
        : BuildItem(
            Source(j),
            j.value("inserted", false),
            j.value("metadataPath", ""))
    { }

    Source source;
    bool inserted = false;
    std::string metadataPath;
};
using Manifest = std::vector<BuildItem>;
void to_json(json& j, const BuildItem& item);
inline void from_json(const json& j, BuildItem& item) { item = BuildItem(j); }

namespace manifest
{
SourceInfo combine(SourceInfo agg, const SourceInfo& info);
SourceInfo combine(SourceInfo agg, Source source);
SourceInfo reduce(const SourceList& list);
SourceInfo merge(SourceInfo a, const SourceInfo& b);
} // namespace manifest

inline bool hasStats(const BuildItem& item)
{
    return hasStats(item.source.info.schema);
}

inline bool isInserted(const BuildItem& item)
{
    return item.inserted;
}

json toOverview(const Manifest& manifest);

// If the stems of all the point cloud paths are unique, then those will be used
// as metadata paths.  Otherwise they'll be stringified origin IDs.
Manifest assignMetadataPaths(Manifest manifest);

void saveEach(
    const Manifest& manifest,
    const arbiter::Endpoint& ep,
    unsigned threads,
    bool pretty = true);

namespace manifest
{

Manifest load(
    const arbiter::Endpoint& ep,
    unsigned threads,
    std::string postfix = "");

Manifest merge(Manifest manifest, const Manifest& other);

} // namespace manifest

} // namespace entwine

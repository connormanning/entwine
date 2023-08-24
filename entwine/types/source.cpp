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
#include <entwine/util/fs.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

namespace
{

bool areStemsUnique(const SourceList& sources)
{
    std::set<std::string> stems;
    for (const auto& source : sources)
    {
        const std::string stem = getStem(source.path);
        if (stem == "manifest" || !stems.insert(stem).second) return false;
    }
    return true;
}

bool areStemsUnique(const Manifest& manifest)
{
    std::set<std::string> stems;
    for (const auto& item : manifest)
    {
        const std::string stem = getStem(item.source.path);

        // manifest.json is reserved for our overview - if there happens to be a
        // file called manifest.whatever then use origin IDs for metadata paths.
        if (stem == "manifest" || !stems.insert(stem).second) return false;
    }
    return true;
}

} // unnamed namespace

void to_json(json& j, const SourceInfo& info)
{
    j = json::object();

    if (!info.pipeline.is_null()) j["pipeline"] = info.pipeline;
    if (info.warnings.size()) j["warnings"] = info.warnings;
    if (info.errors.size()) j["errors"] = info.errors;
    j["points"] = info.points;

    // If we have no points, then our SRS, bounds, and dimensions are not
    // applicable.
    if (!info.points) return;

    j.update({
        { "srs", info.srs },
        { "bounds", info.bounds },
        { "schema", info.schema },
    });

    if (!info.metadata.is_null()) j.update({ { "metadata", info.metadata } });
}

void to_json(json& j, const Source& source)
{
    j = source.info;
    j.update({ { "path", source.path } });
}

void to_json(json& j, const BuildItem& item)
{
    j = item.source;
    j.update({ { "inserted", item.inserted } });
}

SourceInfo::SourceInfo(const json& j)
    : errors(j.value("errors", StringList()))
    , warnings(j.value("warnings", StringList()))
    , pipeline(j.value("pipeline", json()))
    , srs(j.value("srs", Srs()))
    , bounds(j.value("bounds", Bounds()))
    , points(j.value("points", 0))
    , schema(j.value("schema", Schema()))
    , metadata(j.value("metadata", json()))
{ }

SourceInfo manifest::combine(SourceInfo agg, const SourceInfo& cur)
{
    agg.errors.insert(agg.errors.end(), cur.errors.begin(), cur.errors.end());
    agg.warnings.insert(
        agg.warnings.end(),
        cur.warnings.begin(),
        cur.warnings.end());

    if (!cur.points) return agg;

    agg.metadata = json();
    if (cur.srs.exists())
    {
        if (agg.srs.empty()) agg.srs = cur.srs;
        else if (agg.srs != cur.srs)
        {
            const std::string message = "Multiple spatial references found";
            const auto pred = [message](const std::string& s)
            {
                return s == message;
            };
            if (std::none_of(agg.warnings.begin(), agg.warnings.end(), pred))
            {
                agg.warnings.push_back(message);
            }
        }
    }
    agg.bounds.grow(cur.bounds);
    agg.points += cur.points;
    agg.schema = combine(agg.schema, cur.schema);

    return agg;
}

SourceInfo manifest::combine(SourceInfo agg, Source source)
{
    for (auto& w : source.info.warnings) w = source.path + ": " + w;
    for (auto& e : source.info.errors) e = source.path + ": " + e;
    return combine(agg, source.info);
}

SourceInfo manifest::reduce(const SourceList& list)
{
    SourceInfo initial;
    initial.bounds = Bounds::expander();
    return std::accumulate(
        list.begin(),
        list.end(),
        initial,
        [](SourceInfo info, Source source) { return combine(info, source); }
    );
}

json toOverview(const Manifest& manifest)
{
    json j = json::array();
    for (const auto& item : manifest)
    {
        const auto& info = item.source.info;
        json entry = {
            { "path", item.source.path },
            { "metadataPath", item.metadataPath },
            { "inserted", item.inserted },
            { "bounds", info.bounds },
            { "points", info.points }
        };
        if (info.warnings.size()) entry["warnings"] = info.warnings;
        if (info.errors.size()) entry["errors"] = info.errors;

        j.push_back(entry);
    }
    return j;
}

Manifest assignMetadataPaths(Manifest manifest)
{
    const bool stemsAreUnique = areStemsUnique(manifest);
    uint64_t i = 0;

    for (auto& item : manifest)
    {
        const std::string stem = stemsAreUnique
            ? getStem(item.source.path)
            : std::to_string(i);
        item.metadataPath = stem + ".json";

        ++i;
    }

    return manifest;
}

void saveMany(
    const SourceList& sources,
    const arbiter::Endpoint& endpoint,
    const unsigned threads,
    const bool pretty)
{
    if (endpoint.isLocal()) arbiter::mkdirp(endpoint.root());
    saveEach(sources, endpoint, threads, pretty);
}

void saveEach(
    const SourceList& sources,
    const arbiter::Endpoint& ep,
    const unsigned threads,
    const bool pretty)
{
    const bool stemsAreUnique = areStemsUnique(sources);

    uint64_t i = 0;
    Pool pool(threads);
    for (const Source& source : sources)
    {
        const std::string stem = stemsAreUnique
            ? getStem(source.path)
            : std::to_string(i);

        pool.add([&ep, &source, stem, pretty]()
        {
            ensurePut(ep, stem + ".json", json(source).dump(getIndent(pretty)));
        });

        ++i;
    }

    pool.join();
}

void saveEach(
    const Manifest& manifest,
    const arbiter::Endpoint& ep,
    const unsigned threads,
    const bool pretty)
{
    Pool pool(threads);

    for (const auto& item : manifest)
    {
        pool.add([&ep, &item, pretty]()
        {
            ensurePut(
                ep,
                item.metadataPath,
                json(item.source).dump(getIndent(pretty)));
        });
    }

    pool.join();
}

Manifest manifest::load(
    const arbiter::Endpoint& ep,
    const unsigned threads,
    const std::string postfix,
    const bool verbose)
{
    Manifest manifest =
        json::parse(ensureGet(ep, "manifest" + postfix + ".json"));

    Pool pool(threads);
    for (auto& entry : manifest)
    {
        if (entry.metadataPath.size())
        {
            if (verbose)
            {
                std::cout << "Loading " << entry.metadataPath << " from " <<
                    ep.prefixedRoot() << std::endl;
            }
            const json metadata =
                json::parse(ensureGet(ep, entry.metadataPath));
            entry = BuildItem(entwine::merge(json(entry), metadata));
        }
    }
    pool.join();
    return manifest;
}

Manifest manifest::merge(Manifest dst, const Manifest& src)
{
    if (dst.size() != src.size())
    {
        throw std::runtime_error("Manifest sizes do not match");
    }

    for (uint64_t i = 0; i < dst.size(); ++i)
    {
        auto& dstEntry = dst[i];
        const auto& srcEntry = src[i];

        auto& dstInfo = dstEntry.source.info;
        const auto& srcInfo = srcEntry.source.info;

        dstInfo.points += srcInfo.points;

        if (dstEntry.source.path != srcEntry.source.path)
        {
            throw std::runtime_error(
                "Manifest mismatch at origin " + std::to_string(i));
        }

        if (srcEntry.inserted)
        {
            if (!dstEntry.inserted) dstEntry = srcEntry;
            else
            {
                if (srcInfo.points) 
                {
                    dstInfo.schema = combine(dstInfo.schema, srcInfo.schema);
                }

                dstInfo.errors.insert(
                    dstInfo.errors.end(),
                    srcInfo.errors.begin(),
                    srcInfo.errors.end());
                dstInfo.warnings.insert(
                    dstInfo.warnings.end(),
                    srcInfo.warnings.begin(),
                    srcInfo.warnings.end());
            }
        }
    }

    return dst;
}

uint64_t getInsertedPoints(const Manifest& manifest)
{
    return std::accumulate(
        manifest.begin(),
        manifest.end(),
        uint64_t(0),
        [](const uint64_t n, const BuildItem& b)
        {
            if (b.inserted) return n + b.source.info.points;
            return n;
        }
    );
}

uint64_t getTotalPoints(const Manifest& manifest)
{
    return std::accumulate(
        manifest.begin(),
        manifest.end(),
        uint64_t(0),
        [](const uint64_t n, const BuildItem& b)
        {
            return n + b.source.info.points;
        }
    );
}

} // namespace entwine

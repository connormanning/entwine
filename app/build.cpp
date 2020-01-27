/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "build.hpp"

#include <entwine/builder/builder.hpp>
#include <entwine/builder/hierarchy.hpp>
#include <entwine/types/exceptions.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/util/config.hpp>
#include <entwine/util/fs.hpp>
#include <entwine/util/info.hpp>
#include <entwine/util/time.hpp>

namespace entwine
{
namespace app
{

void Build::addArgs()
{
    m_ap.setUsage("entwine build (<options>)");

    addInput(
            "File paths or directory entries.  For a recursive directory "
            "search, the notation is \"directory**\".  May also be the path "
            "to an `entwine scan` output file.\n"
            "Example: -i path.laz, -i pointclouds/, -i autzen/ept-scan.json");

    addOutput(
            "Output directory.\n"
            "Example: --output ~/entwine/autzen");

    addConfig();
    addTmp();
    addReprojection();

    m_ap.add(
            "--threads",
            "-t",
            "The number of threads.\n"
            "Example: --threads 12",
            [this](json j)
            {
                m_json["threads"] = json::parse(j.get<std::string>());
            });

    m_ap.add(
            "--force",
            "-f",
            "Force build overwrite - do not continue a previous build that may "
            "exist at this output location.",
            [this](json j) { checkEmpty(j); m_json["force"] = true; });

    m_ap.add(
            "--dataType",
            "Data type for serialized point cloud data.  Valid values are "
            "\"laszip\", \"zstandard\", or \"binary\".  Default: \"laszip\".\n"
            "Example: --dataType binary",
            [this](json j) { m_json["dataType"] = j; });

    m_ap.add(
            "--span",
            "Number of voxels in each spatial dimension for data nodes.  "
            "For example, a span of 256 will result in a cube of 256*256*256 "
            "resolution.  Default: 256.\n"
            "Example: --span 128",
            [this](json j) { m_json["span"] = extract(j); });

    m_ap.add(
            "--noOriginId",
            "If present, an OriginId dimension tracking points to their "
            "original source files will *not* be inserted.",
            [this](json j)
            {
                checkEmpty(j);
                m_json["allowOriginId"] = false;
            });

    m_ap.add(
            "--bounds",
            "-b",
            "XYZ bounds specification beyond which points will be discarded.  "
            "Format is [xmin, ymin, zmin, xmax, ymax, zmax].\n"
            "Example: --bounds 0 0 0 100 100 100, -b \"[0,0,0,100,100,100]\"",
            [this](json j)
            {
                if (j.is_string())
                {
                    m_json["bounds"] = json::parse(j.get<std::string>());
                }
                else if (j.is_array())
                {
                    for (json& coord : j)
                    {
                        coord = std::stod(coord.get<std::string>());
                    }
                    m_json["bounds"] = j;
                }
            });

    addDeep();
    addAbsolute();

    m_ap.add(
            "--scale",
            "The scale factor for spatial coordinates.\n"
            "Example: --scale 0.1, --scale \"[0.1, 0.1, 0.025]\"",
            [this](json j)
            {
                m_json["scale"] = json::parse(j.get<std::string>());
            });

    m_ap.add(
            "--limit",
            "Maximum number of files to insert - the build may be continued "
            "with another `build` invocation.\n"
            "Example: --limit 20",
            [this](json j) { m_json["limit"] = extract(j); });

    m_ap.add(
            "--subset",
            "-s",
            "A partial task specification for this build.\n"
            "Example: --subset 1 4",
            [this](json j)
            {
                if (!j.is_array() || j.size() != 2)
                {
                    throw std::runtime_error("Invalid subset specification");
                }
                const uint64_t id(std::stoul(j.at(0).get<std::string>()));
                const uint64_t of(std::stoul(j.at(1).get<std::string>()));
                m_json["subset"]["id"] = id;
                m_json["subset"]["of"] = of;
            });

    m_ap.add(
            "--maxNodeSize",
            "Maximum number of points in a node before an overflow is "
            "attempted.",
            [this](json j) { m_json["maxNodeSize"] = extract(j); });

    m_ap.add(
            "--minNodeSize",
            "Minimum number of overflowed points to be retained in a node "
            "before overflowing into a new node.",
            [this](json j) { m_json["minNodeSize"] = extract(j); });

    m_ap.add(
            "--cacheSize",
            "Number of nodes to cache in memory before serializing to the "
            "output.",
            [this](json j) { m_json["cacheSize"] = extract(j); });

    m_ap.add(
            "--hierarchyStep",
            "Hierarchy step size - recommended to be set for testing only as "
            "entwine will determine it heuristically.",
            [this](json j) { m_json["hierarchyStep"] = extract(j); });

    m_ap.add(
            "--sleepCount",
            "Count (per-thread) after which idle nodes are serialized.",
            [this](json j) { m_json["sleepCount"] = extract(j); });

    m_ap.add(
            "--progress",
            "Interval in seconds at which to log build stats.  0 for no "
            "logging (default: 10).",
            [this](json j) { m_json["progressInterval"] = extract(j); });

    addArbiter();
}

void Build::run()
{
    const Endpoints endpoints = config::getEndpoints(m_json);
    const unsigned threads = config::getThreads(m_json);

    Manifest manifest;
    Hierarchy hierarchy;

    // TODO: Handle subset postfixing during existence check - currently
    // continuations of subset builds will not work properly.
    // const optional<Subset> subset = config::getSubset(m_json);
    if (!config::getForce(m_json) && endpoints.output.tryGetSize("ept.json"))
    {
        std::cout << "Awakening existing build." << std::endl;

        // Merge in our metadata JSON, overriding any config settings.
        const json existingConfig = merge(
            json::parse(endpoints.output.get("ept-build.json")),
            json::parse(endpoints.output.get("ept.json"))
        );
        m_json = merge(m_json, existingConfig);

        // Awaken our existing manifest and hierarchy.
        manifest = manifest::load(endpoints.sources, threads);
        hierarchy = hierarchy::load(endpoints.hierarchy, threads);
    }

    // Now, analyze the incoming `input` if needed.
    StringList inputs = resolve(config::getInput(m_json), *endpoints.arbiter);
    const auto exists = [&manifest](std::string path)
    {
        return std::any_of(
            manifest.begin(),
            manifest.end(),
            [path](const BuildItem& b) { return b.source.path == path; });
    };
    // Remove any inputs we already have in our manifest prior to analysis.
    inputs.erase(
        std::remove_if(inputs.begin(), inputs.end(), exists),
        inputs.end());
    const SourceList sources = analyze(
        inputs,
        config::getPipeline(m_json),
        config::getDeep(m_json),
        config::getTmp(m_json),
        *endpoints.arbiter,
        threads);
    for (const auto& source : sources) manifest.emplace_back(source);

    // It's possible we've just analyzed some files, in which case we have
    // potentially new information like bounds, schema, and SRS.  Prioritize
    // values from the config, which may explicitly override these.
    const SourceInfo analysis = manifest::reduce(sources);
    m_json = merge(analysis, m_json);
    const Metadata metadata = config::getMetadata(m_json);

    Builder builder(endpoints, metadata, manifest, hierarchy);

    const uint64_t points = getTotalPoints(manifest);
    printInfo(
        metadata.schema,
        metadata.boundsConforming,
        metadata.srs.value_or(Srs()),
        points,
        analysis.warnings,
        analysis.errors);
    if (metadata.subset)
    {
        std::cout << "Subset: " <<
            metadata.subset->id << "/" <<
            metadata.subset->of << std::endl;
    }

    std::cout << std::endl;

    const uint64_t actual = builder.run(
        config::getCompoundThreads(m_json),
        config::getLimit(m_json),
        config::getProgressInterval(m_json));

    std::cout << "Wrote " << commify(actual) << " points." << std::endl;
}

} // namespace app
} // namespace entwine

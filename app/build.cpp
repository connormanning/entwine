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

    m_ap.add(
            "--laz_14",
            "Write LAZ 1.4 content encoding (default: false)",
            [this](json j)
            {
                checkEmpty(j);
                m_json["laz_14"] = true;
            });

    addArbiter();
}

void Build::run()
{
    Builder builder = builder::create(m_json);
    const Metadata metadata = builder.metadata;
    const Manifest manifest = builder.manifest;

    SourceList sources;
    for (const auto& item : manifest) sources.push_back(item.source);
    const auto analysis = manifest::reduce(sources);

    if (getInsertedPoints(manifest))
    {
        std::cout << "Continuing existing build." << std::endl;
    }
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

    const uint64_t actual = builder::run(builder, m_json);

    for (const auto& f : builder.manifest)
    {
        for (const auto& e : f.source.info.errors)
        {
            std::cout << "\t" << f.source.path << ": " << e << std::endl;
        }
    }

    std::cout << "Wrote " << commify(actual) << " points." << std::endl;
}

} // namespace app
} // namespace entwine

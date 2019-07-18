/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "build.hpp"

#include <chrono>
#include <fstream>
#include <iostream>
#include <string>

#include <entwine/builder/builder.hpp>
#include <entwine/builder/thread-pools.hpp>
#include <entwine/io/io.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/files.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/env.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/matrix.hpp>

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
            "Example: -i path.laz, -i pointclouds/, -i autzen/scan.json");

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

    addNoTrustHeaders();
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
            "--run",
            "-g",
            "Maximum number of files to insert - the build may be continued "
            "with another `build` invocation.\n"
            "Example: --run 20",
            [this](json j) { m_json["run"] = extract(j); });

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
            "--overflowDepth",
            "Depth at which nodes may overflow.",
            [this](json j) { m_json["overflowDepth"] = extract(j); });

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
    m_json["verbose"] = true;

    Config config(m_json);
    auto builder(makeUnique<Builder>(config));

    log(*builder);

    const Files& files(builder->metadata().files());

    auto start = now();
    const uint64_t alreadyInserted(files.pointStats().inserts());

    const uint64_t runCount(m_json.value("run", 0u));
    builder->go(runCount);

    std::cout << "\nIndex completed in " <<
        formatTime(since<std::chrono::seconds>(start)) << "." << std::endl;

    std::cout << "Save complete.\n";

    const PointStats stats(files.pointStats());

    if (alreadyInserted)
    {
        std::cout <<
            "\tPoints inserted:\n" <<
            "\t\tPreviously: " << commify(alreadyInserted) << "\n" <<
            "\t\tCurrently:  " <<
                commify((stats.inserts() - alreadyInserted)) << "\n" <<
            "\t\tTotal:      " << commify(stats.inserts()) << std::endl;
    }
    else
    {
        std::cout << "\tPoints inserted: " << commify(stats.inserts()) << "\n";
    }

    if (stats.outOfBounds())
    {
        std::cout <<
            "\tPoints discarded: " << commify(stats.outOfBounds()) << "\n" <<
            std::endl;
    }

    end(*builder);
}

void Build::end(const Builder& b) const
{
    const FileInfoList& list(b.metadata().files().list());

    const bool error(std::any_of(list.begin(), list.end(), [](const FileInfo& f)
    {
        return f.status() == FileInfo::Status::Error;
    }));

    if (!error) return;

    std::cout << "\nErrors encountered - data may be missing.  Errors:" <<
        std::endl;

    int i(0);
    for (const auto& f : list)
    {
        if (f.status() == FileInfo::Status::Error)
        {
            std::cout << "\t" << ++i << " - " << f.path() << ": " <<
                f.message() << std::endl;
        }
    }
}

void Build::log(const Builder& b) const
{
    if (b.isContinuation())
    {
        std::cout << "\nContinuing previous index..." << std::endl;
    }

    std::string outPath(b.outEndpoint().prefixedRoot());
    std::string tmpPath(b.tmpEndpoint().root());

    const Metadata& metadata(b.metadata());
    const Files& files(metadata.files());
    const Reprojection* reprojection(metadata.reprojection());
    const Schema& schema(metadata.outSchema());
    const uint64_t runCount(m_json.value("run", 0u));

    std::cout << std::endl;
    std::cout <<
        "Entwine Version: " << currentEntwineVersion().toString() << "\n" <<
        "EPT Version: " << currentEptVersion().toString() << "\n" <<
        "Input:\n\t";

    if (files.size() == 1)
    {
        std::cout << "File: " << files.get(0).path() << std::endl;
    }
    else
    {
        std::cout << "Files: " << files.size() << std::endl;
    }

    if (runCount)
    {
        std::cout <<
            "\tInserting up to " << runCount << " file" <<
            (runCount > 1 ? "s" : "") << "\n";
    }

    std::cout << "\tTotal points: " <<
        commify(metadata.files().totalPoints()) << std::endl;

    std::cout << "\tDensity estimate (per square unit): " <<
        densityLowerBound(metadata.files().list()) << std::endl;

    if (!metadata.trustHeaders())
    {
        std::cout << "\tTrust file headers? " << yesNo(false) << "\n";
    }

    std::cout <<
        "\tThreads: [" <<
            b.threadPools().workPool().numThreads() << ", " <<
            b.threadPools().clipPool().numThreads() << "]" <<
        std::endl;

    std::cout <<
        "Output:\n" <<
        "\tPath: " << outPath << "\n" <<
        "\tData type: " << metadata.dataIo().type() << "\n" <<
        "\tHierarchy type: " << "json" << "\n" <<
        "\tSleep count: " << commify(b.sleepCount()) <<
        std::endl;

    if (schema.isScaled())
    {
        std::cout << "\tScale: ";
        const Scale scale(schema.scale());
        if (scale.x == scale.y && scale.x == scale.z)
        {
            std::cout << scale.x << std::endl;
        }
        else
        {
            std::cout << scale << std::endl;
        }
        std::cout << "\tOffset: " << schema.offset() << std::endl;
    }
    else
    {
        std::cout << "\tScale: (absolute)" << std::endl;
        std::cout << "\tOffset: (none)" << std::endl;
    }

    std::cout << "Metadata:\n";

    const auto& srs(metadata.srs());
    std::cout << "\tSRS: ";
    if (srs.hasCode()) std::cout << srs.codeString();
    else if (!srs.empty())
    {
        const auto s(srs.wkt());
        if (s.size() < 60) std::cout << s;
        else std::cout << s.substr(0, 60) + " ...";
    }
    else std::cout << "(none)";
    std::cout << "\n";

    std::cout <<
        "\tBounds: " << metadata.boundsConforming() << "\n" <<
        "\tCube: " << metadata.boundsCubic() << "\n";

    if (const Subset* s = metadata.subset())
    {
        std::cout << "\tSubset bounds: " << s->bounds() << "\n";
    }

    if (reprojection)
    {
        std::cout << "\tReprojection: " << *reprojection << "\n";
    }

    std::cout << "\tStoring dimensions: " << getDimensionString(schema) << "\n";

    const auto t(metadata.span());
    std::cout << "Build parameters:\n" <<
        "\tSpan: " << t << "\n" <<
        "\tResolution 2D: " <<
            t << " * " << t << " = " << commify(t * t) << "\n" <<
        "\tResolution 3D: " <<
            t << " * " << t << " * " << t << " = " << commify(t * t * t) <<
            "\n" <<
        "\tMaximum node size: " << commify(metadata.maxNodeSize()) << "\n" <<
        "\tMinimum node size: " << commify(metadata.minNodeSize()) << "\n" <<
        "\tCache size: " << commify(metadata.cacheSize()) << "\n";

    if (const Subset* s = metadata.subset())
    {
        std::cout << "\tSubset: " << s->id() << " of " << s->of() << "\n";
    }

    std::cout << std::endl;
}

} // namespace app
} // namespace entwine


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
            "search, the notation is 'directory**'.  May also be the path "
            "to an `entwine scan` output file\n"
            "Example: --input path.laz, -i pointclouds/, -i scan.json");

    addOutput(
            "Output directory.\n"
            "Example: --output ~/entwine/autzen");

    addConfig();
    addTmp();
    addReprojection();

    m_ap.add(
            "--threads",
            "-t",
            "The number of threads\n"
            "Example: --threads 12",
            [this](Json::Value v) { m_json["threads"] = parse(v.asString()); });

    m_ap.add(
            "--force",
            "-f",
            "Force build overwrite - do not continue a previous build that may "
            "exist at this output location",
            [this](Json::Value v) { checkEmpty(v); m_json["force"] = true; });

    m_ap.add(
            "--dataType",
            "Data type for serialized point cloud data.  Valid values are "
            "\"laszip\" or \"binary\".  Default: \"laszip\".\n"
            "Example: --dataType binary",
            [this](Json::Value v) { m_json["dataType"] = v.asString(); });

    m_ap.add(
            "--ticks",
            "Number of grid ticks in each spatial dimensions for data nodes.  "
            "For example, a \"ticks\" value of 256 will result in a cube of "
            "256*256*256 resolution.  Default: 256.\n"
            "Example: --ticks 128",
            [this](Json::Value v) { m_json["ticks"] = extract(v); });

    m_ap.add(
            "--noOriginId",
            "If present, an OriginId dimension tracking points to their "
            "original source files will *not* be inserted.",
            [this](Json::Value v)
            {
                checkEmpty(v);
                m_json["allowOriginId"] = false;
            });

    m_ap.add(
            "--bounds",
            "-b",
            "XYZ bounds specification beyond which points will be discarded\n"
            "Example: --bounds 0 0 0 100 100 100, -b \"[0,0,0,100,100,100]\"",
            [this](Json::Value v)
            {
                if (v.isString()) m_json["bounds"] = parse(v.asString());
                else if (v.isArray())
                {
                    for (Json::ArrayIndex i(0); i < v.size(); ++i)
                    {
                        v[i] = std::stod(v[i].asString());
                    }
                    m_json["bounds"] = v;
                }
            });

    addNoTrustHeaders();
    addAbsolute();

    m_ap.add(
            "--scale",
            "The scale factor for spatial coordinates.\n"
            "Example: --scale 0.1, --scale \"[0.1, 0.1, 0.025]\"",
            [this](Json::Value v) { m_json["scale"] = parse(v.asString()); });

    m_ap.add(
            "--run",
            "-g",
            "Maximum number of files to insert - the build may be continued "
            "with another `build` invocation\n"
            "Example: --run 20",
            [this](Json::Value v) { m_json["run"] = extract(v); });

    m_ap.add(
            "--resetFiles",
            "Reset the memory pool after \"n\" files\n"
            "Example: --resetFiles 100",
            [this](Json::Value v) { m_json["resetFiles"] = extract(v); });

    m_ap.add(
            "--subset",
            "-s",
            "A partial task specification for this build\n"
            "Example: --subset 1 4",
            [this](Json::Value v)
            {
                if (!v.isArray() || v.size() != 2)
                {
                    throw std::runtime_error("Invalid subset specification");
                }
                const Json::UInt64 id(std::stoul(v[0].asString()));
                const Json::UInt64 of(std::stoul(v[1].asString()));
                m_json["subset"]["id"] = id;
                m_json["subset"]["of"] = of;
            });

    m_ap.add(
            "--overflowDepth",
            "Depth at which nodes may overflow",
            [this](Json::Value v) { m_json["overflowDepth"] = extract(v); });

    m_ap.add(
            "--overflowThreshold",
            "Threshold at which overflowed points are placed into child nodes",
            [this](Json::Value v)
            {
                m_json["overflowThreshold"] = extract(v);
            });

    m_ap.add(
            "--hierarchyStep",
            "Hierarchy step size - recommended to be set for testing only as "
            "entwine will determine it heuristically.",
            [this](Json::Value v) { m_json["hierarchyStep"] = extract(v); });

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
    const std::size_t alreadyInserted(files.pointStats().inserts());

    const std::size_t runCount(m_json["run"].asUInt64());
    builder->go(runCount);

    std::cout << "\nIndex completed in " <<
        commify(since<std::chrono::seconds>(start)) << " seconds." <<
        std::endl;

    std::cout << "Save complete.  Indexing stats:\n";

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
            "\tPoints discarded:\n" << commify(stats.outOfBounds()) << "\n" <<
            std::endl;
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
    const Schema& schema(metadata.schema());
    const std::size_t runCount(m_json["run"].asUInt64());

    std::cout << std::endl;
    std::cout <<
        "Version: " << currentVersion().toString() << "\n" <<
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

    if (uint64_t rf = b.resetFiles())
    {
        std::cout << "\tReset files: " << rf << std::endl;
    }

    const auto hs(metadata.hierarchyStep());
    std::cout <<
        "Output:\n" <<
        "\tPath: " << outPath << "\n" <<
        "\tData type: " << metadata.dataIo().type() << "\n" <<
        "\tHierarchy type: " << "json" << "\n" <<
        "\tHierarchy step: " << (hs ? std::to_string(hs) : "auto") << "\n" <<
        "\tSleep count: " << b.sleepCount() <<
        std::endl;

    if (const auto* delta = metadata.delta())
    {
        std::cout << "\tScale: ";
        const auto& scale(delta->scale());
        if (scale.x == scale.y && scale.x == scale.z)
        {
            std::cout << scale.x << std::endl;
        }
        else
        {
            std::cout << scale << std::endl;
        }
        std::cout << "\tOffset: " << delta->offset() << std::endl;
    }
    else
    {
        std::cout << "\tScale: (absolute)" << std::endl;
        std::cout << "\tOffset: (0, 0, 0)" << std::endl;
    }

    std::cout <<
        "Metadata:\n" <<
        "\tBounds: " << metadata.boundsConforming() << "\n" <<
        "\tCube: " << metadata.boundsCubic() << "\n";

    if (const Subset* s = metadata.subset())
    {
        std::cout << "\tSubset bounds: " << s->bounds() << "\n";
    }

    std::cout <<
        "\tReprojection: " <<
            (reprojection ? reprojection->toString() : "(none)") << "\n" <<
        "\tStoring dimensions: " << getDimensionString(schema) << std::endl;

    const auto t(metadata.ticks());
    std::cout << "Build parameters:\n" <<
        "\tTicks: " << t << "\n" <<
        "\tResolution 2D: " <<
            t << " * " << t << " = " << commify(t * t) << "\n" <<
        "\tResolution 3D: " <<
            t << " * " << t << " * " << t << " = " << commify(t * t * t) <<
            "\n" <<
        "\tOverflow threshold: " << commify(metadata.overflowThreshold()) <<
            "\n";

    if (const Subset* s = metadata.subset())
    {
        std::cout << "\tSubset: " << s->id() << " of " << s->of() << "\n";
    }

    if (metadata.transformation())
    {
        std::cout << "\tTransformation: ";
        matrix::print(*metadata.transformation(), 0, "\t");
    }

    std::cout << std::endl;
}

} // namespace app
} // namespace entwine


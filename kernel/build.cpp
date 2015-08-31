/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "entwine.hpp"

#include <fstream>
#include <iostream>
#include <string>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/config-parser.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/fs.hpp>

using namespace entwine;

namespace
{
    std::string yesNo(const bool val)
    {
        return (val ? "yes" : "no");
    }

    std::chrono::high_resolution_clock::time_point now()
    {
        return std::chrono::high_resolution_clock::now();
    }

    int secondsSince(const std::chrono::high_resolution_clock::time_point start)
    {
        std::chrono::duration<double> d(now() - start);
        return std::chrono::duration_cast<std::chrono::seconds>(d).count();
    }

    std::string getUsageString()
    {
        return
            "\tUsage: entwine build <config-file.json> <options>\n"
            "\tOptions:\n"

            "\t\t-c <credentials-path.json>\n"
            "\t\t\tSpecify path to AWS S3 credentials\n"

            "\t\t-f\n"
            "\t\t\tForce build overwrite - do not continue previous build\n"

            "\t\t-s <subset-number> <subset-total>\n"
            "\t\t\tBuild only a portion of the index.  If output paths are\n"
            "\t\t\tall the same, 'merge' should be run after all subsets are\n"
            "\t\t\tbuilt.  If output paths are different, then 'link' should\n"
            "\t\t\tbe run after all subsets are built.\n\n"
            "\t\t\tsubset-number - One-based subset ID in range\n"
            "\t\t\t[1, subset-total].\n\n"
            "\t\t\tsubset-total - Total number of subsets that will be built.\n"
            "\t\t\tMust be 4, 16, or 64.\n";
    }

    std::string getDimensionString(const Schema& schema)
    {
        const DimList dims(schema.dims());
        std::string results("[");

        for (std::size_t i(0); i < dims.size(); ++i)
        {
            if (i) results += ", ";
            results += dims[i].name();
        }

        results += "]";

        return results;
    }

    std::string getBBoxString(const BBox* bbox, const std::size_t dimensions)
    {
        if (bbox)
        {
            std::ostringstream oss;
            oss << *bbox;
            return oss.str();
        }
        else
        {
            return "(inferring from source)";
        }
    }

    std::string getReprojString(const Reprojection* reprojection)
    {
        if (reprojection)
        {
            return reprojection->in() + " -> " + reprojection->out();
        }
        else
        {
            return "(none)";
        }
    }
}

void Kernel::build(std::vector<std::string> args)
{
    if (args.empty())
    {
        std::cout << getUsageString() << std::endl;
        throw std::runtime_error("No config file specified.");
    }

    if (args[0] == "help")
    {
        std::cout << getUsageString() << std::endl;
        return;
    }

    arbiter::Arbiter localArbiter;

    const std::string configPath(args[0]);
    const std::string config(localArbiter.get(configPath));
    Json::Value json(ConfigParser::parse(config));

    std::string credPath;

    std::size_t a(1);

    while (a < args.size())
    {
        std::string arg(args[a]);

        if (arg == "-c")
        {
            if (++a < args.size())
            {
                credPath = args[a];
            }
            else
            {
                throw std::runtime_error("Invalid credential path argument");
            }
        }
        else if (arg == "-f")
        {
            json["output"]["force"] = true;
        }
        else if (arg == "-s")
        {
            if (a + 2 < args.size())
            {
                ++a;
                const Json::UInt64 id(std::stoul(args[a]));
                ++a;
                const Json::UInt64 of(std::stoul(args[a]));

                json["structure"]["subset"]["id"] = id;
                json["structure"]["subset"]["of"] = of;
            }
            else
            {
                throw std::runtime_error("Invalid subset specification");
            }
        }

        ++a;
    }

    const std::string creds(credPath.size() ? localArbiter.get(credPath) : "");
    auto arbiter(ConfigParser::getArbiter(ConfigParser::parse(creds)));

    RunInfo runInfo(ConfigParser::getRunInfo(json, *arbiter));
    std::unique_ptr<Builder> builder(
            ConfigParser::getBuilder(json, arbiter, runInfo));

    if (builder->isContinuation())
    {
        std::cout << "\nContinuing previous index..." << std::endl;
    }

    const auto& outEndpoint(builder->outEndpoint());
    const auto& tmpEndpoint(builder->tmpEndpoint());

    std::string outPath(
            (outEndpoint.type() != "fs" ? outEndpoint.type() + "://" : "") +
            outEndpoint.root());
    std::string tmpPath(tmpEndpoint.root());

    const Structure& structure(builder->structure());

    const BBox* bbox(builder->bbox());
    const Reprojection* reprojection(builder->reprojection());
    const Schema& schema(builder->schema());

    std::cout << std::endl;

    std::cout <<
        "Input:\n" <<
        "\tBuilding from " << runInfo.manifest.size() << " source file" <<
            (runInfo.manifest.size() > 1 ? "s" : "") << "\n";

    if (runInfo.maxCount != runInfo.manifest.size())
    {
        std::cout <<
            "\tInserting up to " << runInfo.maxCount << " file" <<
                (runInfo.maxCount > 1 ? "s" : "") << "\n";
    }

    std::cout <<
        "\tTrust file headers? " << yesNo(builder->trustHeaders()) << "\n" <<
        "\tBuild threads: " << builder->numThreads() <<
        std::endl;

    std::cout <<
        "Output:\n" <<
        "\tOutput path: " << outPath << "\n" <<
        "\tTemporary path: " << tmpPath << "\n" <<
        "\tCompressed output? " << yesNo(builder->compress()) <<
        std::endl;

    std::cout <<
        "Tree structure:\n" <<
        "\tNull depth: " << structure.nullDepthEnd() << "\n" <<
        "\tBase depth: " << structure.baseDepthEnd() << "\n" <<
        "\tCold depth: " << structure.coldDepthEnd() << "\n" <<
        "\tChunk size: " << structure.baseChunkPoints() << " points\n" <<
        "\tDynamic chunks? " << yesNo(structure.dynamicChunks()) << "\n" <<
        "\tBuild type: " << structure.typeString() << "\n" <<
        "\tPoint count hint: " << structure.numPointsHint() << " points" <<
        std::endl;

    std::cout <<
        "Geometry:\n" <<
        "\tBounds: " << getBBoxString(bbox, structure.dimensions()) << "\n" <<
        "\tReprojection: " << getReprojString(reprojection) << "\n" <<
        "\tStoring dimensions: " << getDimensionString(schema) << "\n" <<
        std::endl;

    if (structure.subset())
    {
        const auto subset(structure.subset());
        std::cout << "Subset: " <<
            subset->id() + 1 << " of " << subset->of() << "\n" <<
            std::endl;
    }

    // Index.
    std::size_t i(0);
    std::size_t numInserted(0);

    auto start = now();

    const auto& manifest(runInfo.manifest);
    const std::size_t maxCount(runInfo.maxCount);

    while (i < manifest.size() && numInserted < maxCount)
    {
        if (builder->insert(manifest[i++]))
        {
            ++numInserted;
        }
    }

    std::cout << "Joining..." << std::endl;

    builder->join();

    std::cout << "\nIndex completed in " << secondsSince(start) <<
            " seconds.  Saving...\n" << std::endl;

    builder->save();

    const Stats stats(builder->stats());
    std::cout <<
        "Save complete.  Indexing stats:\n" <<
        "\tPoints inserted: " << stats.getNumPoints() << "\n" <<
        "\tPoints discarded:\n" <<
        "\t\tOutside specified bounds: " << stats.getNumOutOfBounds() << "\n" <<
        "\t\tOverflow past max depth: " << stats.getNumFallThroughs() << "\n" <<
        std::endl;
}


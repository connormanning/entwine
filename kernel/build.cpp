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
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>

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
            "\nUsage: entwine build <config file> <options>\n"

            "\nConfig file:\n"
            "\tOptional parameter, recommended only if the options below are\n"
            "\tinsufficient.  See template at https://git.io/v2jPQ\n"

            "\nOptions (overrides config values):\n"

            "\t-i <input path>\n"
            "\t\tSpecify the input location.  May end in '/*' for a\n"
            "\t\tnon-recursive directory or '/**' for a recursive search.\n"
            "\t\tMay be type-prefixed, e.g. s3://bucket/data/*.\n\n"

            "\t-o <output path>\n"
            "\t\tOutput directory.\n\n"

            "\t-x <tmp path>\n"
            "\t\tDirectory for entwine-generated temporary files.\n\n"

            "\t-r (<input reprojection>) <output reprojection>\n"
            "\t\tSet the spatial reference system reprojection.  The input\n"
            "\t\tvalue may be omitted to infer the input SRS from the file\n"
            "\t\theader.  In this case the build will fail if no input SRS\n"
            "\t\tmay be inferred.  Reprojection strings may be any of the\n"
            "\t\tformats supported by GDAL.\n\n"

            "\t-t <threads>\n"
            "\t\tSet the number of worker threads.  Recommended to be no\n"
            "\t\tmore than the physical number of cores.\n\n"

            "\t-f\n"
            "\t\tForce build overwrite - do not continue a previous build\n"
            "\t\tthat may exist at this output location.\n\n"

            "\t-u <aws user>\n"
            "\t\tSpecify AWS credential user, if not default\n\n"

            "\t-g <max inserted files>\n"
            "\t\tFor directories, stop inserting after the specified count.\n\n"

            "\t-p\n"
            "\t\tPrefix stored IDs with a SHA (may be useful for filename-based"
            "\t\tdistributed filesystems).\n\n"

            "\t-s <subset-number> <subset-total>\n"
            "\t\tBuild only a portion of the index.  If output paths are\n"
            "\t\tall the same, 'merge' should be run after all subsets are\n"
            "\t\tbuilt.  If output paths are different, then 'link' should\n"
            "\t\tbe run after all subsets are built.\n\n"
            "\t\tsubset-number - One-based subset ID in range\n"
            "\t\t[1, subset-total].\n\n"
            "\t\tsubset-total - Total number of subsets that will be built.\n"
            "\t\tMust be a binary power.\n\n";
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

    const Json::Value defaults(([]()
    {
        Json::Value json;

        json["input"]["manifest"] = Json::Value::null;
        json["input"]["threads"] = 9;
        json["input"]["trustHeaders"] = true;

        json["output"]["path"] = Json::Value::null;
        json["output"]["tmp"] = "tmp";
        json["output"]["compress"] = true;

        json["structure"]["numPointsHint"] = Json::Value::null;
        json["structure"]["nullDepth"] = 6;
        json["structure"]["baseDepth"] = 10;
        json["structure"]["pointsPerChunk"] = 262144;
        json["structure"]["dynamicChunks"] = true;
        json["structure"]["type"] = "hybrid";
        json["structure"]["prefixIds"] = false;

        json["geometry"]["bbox"] = Json::Value::null;
        json["geometry"]["reproject"] = Json::Value::null;
        json["geometry"]["schema"] = Json::Value::null;

        return json;
    })());
}

void Kernel::build(std::vector<std::string> args)
{
    if (args.empty())
    {
        std::cout << getUsageString() << std::flush;
        return;
    }

    if (args[0] == "help" || args[0] == "-h" || args[0] == "--help")
    {
        std::cout << getUsageString() << std::flush;
        return;
    }

    arbiter::Arbiter localArbiter;
    Json::Value json(defaults);
    std::string user;

    std::size_t a(0);

    if (args[0][0] != '-')
    {
        // First argument is a config path.
        const std::string configPath(args[0]);
        const std::string config(localArbiter.get(configPath));
        json = ConfigParser::parse(config);
        ++a;
    }

    std::unique_ptr<Manifest::Split> split;

    while (a < args.size())
    {
        std::string arg(args[a]);

        if (arg == "-i")
        {
            if (++a < args.size())
            {
                json["input"]["manifest"] = args[a];
            }
            else
            {
                throw std::runtime_error("Invalid input path specification");
            }
        }
        else if (arg == "-o")
        {
            if (++a < args.size())
            {
                json["output"]["path"] = args[a];
            }
            else
            {
                throw std::runtime_error("Invalid output path specification");
            }
        }
        else if (arg == "-x")
        {
            if (++a < args.size())
            {
                json["output"]["tmp"] = args[a];
            }
            else
            {
                throw std::runtime_error("Invalid tmp specification");
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

                json["subset"]["id"] = id;
                json["subset"]["of"] = of;
            }
            else
            {
                throw std::runtime_error("Invalid subset specification");
            }
        }
        if (arg == "-u")
        {
            if (++a < args.size())
            {
                user = args[a];
            }
            else
            {
                throw std::runtime_error("Invalid AWS user argument");
            }
        }
        else if (arg == "-r")
        {
            if (++a < args.size())
            {
                const bool onlyOutput(
                        a + 1 >= args.size() ||
                        args[a + 1][0] == '-');

                if (onlyOutput)
                {
                    json["geometry"]["reproject"]["out"] = args[a];
                }
                else
                {
                    json["geometry"]["reproject"]["in"] = args[a];
                    json["geometry"]["reproject"]["out"] = args[++a];
                }
            }
            else
            {
                throw std::runtime_error("Invalid reprojection argument");
            }
        }
        /*
        else if (arg == "-m")
        {
            if (a + 2 < args.size())
            {
                ++a;
                const Json::UInt64 begin(std::stoul(args[a]));
                ++a;
                const Json::UInt64 end(std::stoul(args[a]));

                split.reset(new Manifest::Split(begin, end));
            }
            else
            {
                throw std::runtime_error("Invalid manifest specification");
            }
        }
        */
        else if (arg == "-g")
        {
            if (++a < args.size())
            {
                json["input"]["run"] = Json::UInt64(std::stoul(args[a]));
            }
            else
            {
                throw std::runtime_error("Invalid run count specification");
            }
        }
        else if (arg == "-p")
        {
            json["structure"]["prefixIds"] = true;
        }
        else if (arg == "-t")
        {
            if (++a < args.size())
            {
                json["input"]["threads"] = Json::UInt64(std::stoul(args[a]));
            }
            else
            {
                throw std::runtime_error("Invalid thread count specification");
            }
        }

        ++a;
    }

    Json::Value arbiterConfig(json["arbiter"]);
    arbiterConfig["s3"]["user"] = user;

    std::shared_ptr<arbiter::Arbiter> arbiter(
            std::make_shared<arbiter::Arbiter>(arbiterConfig));

    std::unique_ptr<Manifest> manifest(
            ConfigParser::getManifest(json, *arbiter));

    if (split) manifest->split(split->begin(), split->end());

    std::unique_ptr<Builder> builder(
            ConfigParser::getBuilder(json, arbiter, std::move(manifest)));

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

    const Reprojection* reprojection(builder->reprojection());
    const Schema& schema(builder->schema());
    const std::size_t runCount(json["input"]["run"].asUInt64());

    std::cout << std::endl;

    std::cout <<
        "Input:\n" <<
        "\tBuilding from " << builder->manifest().size() << " source file" <<
            (builder->manifest().size() > 1 ? "s" : "") << "\n";

    if (runCount)
    {
        std::cout <<
            "\tInserting up to " << runCount << " file" <<
            (runCount > 1 ? "s" : "") << "\n";
    }

    const std::string coldDepthString(
            structure.lossless() ?
                "lossless" :
                std::to_string(structure.coldDepthEnd()));

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
        "\tCold depth: " << coldDepthString << "\n" <<
        "\tChunk size: " << structure.baseChunkPoints() << " points\n" <<
        "\tDynamic chunks? " << yesNo(structure.dynamicChunks()) << "\n" <<
        "\tDiscard dupes? " << yesNo(structure.discardDuplicates()) << "\n" <<
        "\tPrefix IDs? " << yesNo(structure.prefixIds()) << "\n" <<
        "\tBuild type: " << structure.typeString() << "\n" <<
        "\tPoint count hint: " << structure.numPointsHint() << " points" <<
        std::endl;

    std::cout <<
        "Geometry:\n" <<
        "\tConforming bounds: " << builder->bboxConforming() << "\n" <<
        "\tCubic bounds: " << builder->bbox() << "\n" <<
        "\tReprojection: " << getReprojString(reprojection) << "\n" <<
        "\tStoring dimensions: " << getDimensionString(schema) << "\n" <<
        std::endl;

    if (const Subset* subset = builder->subset())
    {
        std::cout <<
            "Subset: " << subset->id() + 1 << " of " << subset->of() << "\n" <<
            "Subset bounds: " << subset->bbox() << "\n" <<
            std::endl;
    }

    if (const Manifest::Split* split = builder->manifest().split())
    {
        std::cout <<
            "Manifest split: [" << split->begin() << ", " <<
            split->end() << ")\n" <<
            std::endl;
    }

    auto start = now();
    builder->go(runCount);
    std::cout << "\nIndex completed in " << secondsSince(start) <<
        " seconds." << std::endl;

    const PointStats stats(builder->manifest().pointStats());
    std::cout <<
        "Save complete.  Indexing stats:\n" <<
        "\tPoints inserted: " << stats.inserts() << "\n" <<
        "\tPoints discarded:\n" <<
        "\t\tOutside specified bounds: " << stats.outOfBounds() << "\n" <<
        "\t\tOverflow past max depth: " << stats.overflows() << "\n" <<
        std::endl;
}


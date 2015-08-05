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

#include <entwine/third/json/json.h>
#include <entwine/tree/builder.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/fs.hpp>

using namespace entwine;

namespace
{
    std::size_t getRunCount(const Json::Value& input, std::size_t manifestSize)
    {
        std::size_t runCount(manifestSize);

        if (input.isMember("run") && input["run"].asUInt64() != 0)
        {
            runCount = std::min<std::size_t>(
                    input["run"].asUInt64(),
                    manifestSize);
        }

        return runCount;
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

    std::vector<std::string> getManifest(
            const Json::Value& json,
            arbiter::Arbiter& arbiter)
    {
        std::vector<std::string> manifest;

        auto insert([&manifest, &arbiter](std::string in)
        {
            std::vector<std::string> paths(arbiter.resolve(in, true));
            manifest.insert(manifest.end(), paths.begin(), paths.end());
        });

        if (json.isArray())
        {
            for (Json::ArrayIndex i(0); i < json.size(); ++i)
            {
                insert(json[i].asString());
            }
        }
        else
        {
            insert(json.asString());
        }

        return manifest;
    }

    std::size_t getDimensions(const Json::Value& jsonType)
    {
        const std::string typeString(jsonType.asString());

        if (typeString == "quadtree") return 2;
        else if (typeString == "octree") return 3;
        else throw std::runtime_error("Invalid tree type");
    }

    std::unique_ptr<BBox> getBBox(
            Json::Value& json,
            const std::size_t dimensions)
    {
        std::unique_ptr<BBox> bbox;

        if (!json.empty())
        {
            if (json.size() == 4 && dimensions == 2)
            {
                Json::Value expanded;
                Json::Value& bounds(expanded["bounds"]);

                bounds.append(json[0].asDouble());
                bounds.append(json[1].asDouble());
                bounds.append(std::numeric_limits<double>::max());
                bounds.append(json[2].asDouble());
                bounds.append(json[3].asDouble());
                bounds.append(std::numeric_limits<double>::lowest());

                expanded["is3d"] = false;

                bbox.reset(new BBox(expanded));
            }
            else if (dimensions == 3)
            {
                Json::Value expanded;
                expanded["bounds"] = json;
                expanded["is3d"] = true;
                bbox.reset(new BBox(expanded));
            }
            else
            {
                throw std::runtime_error(
                        "Invalid bbox for the requested tree type.");
            }
        }

        return bbox;
    }

    std::unique_ptr<Reprojection> getReprojection(const Json::Value& json)
    {
        std::unique_ptr<Reprojection> reprojection;

        const Json::Value& in(json["in"]);
        const Json::Value& out(json["out"]);

        if (
                !json.empty() &&
                in.isString() && in.asString().size() &&
                out.isString() && out.asString().size())
        {
            reprojection.reset(new Reprojection(in.asString(), out.asString()));
        }

        return reprojection;
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

    const std::string configFilename(args[0]);
    std::ifstream configStream(configFilename, std::ifstream::binary);

    if (!configStream.good())
    {
        std::cout << getUsageString() << std::endl;
        throw std::runtime_error("Couldn't open " + configFilename);
    }

    std::string credPath("credentials.json");
    bool force(false);
    std::pair<std::size_t, std::size_t> subset({ 0, 0 });

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
            force = true;
        }
        else if (arg == "-s")
        {
            if (a + 2 < args.size())
            {
                ++a;
                const std::size_t num(std::stoul(args[a]) - 1);
                ++a;
                const std::size_t cnt(std::stoul(args[a]));

                if (num < cnt && (cnt == 4 || cnt == 16 || cnt == 64))
                {
                    subset = { num, cnt };
                }
                else
                {
                    throw std::runtime_error("Invalid subset values");
                }
            }
            else
            {
                throw std::runtime_error("Invalid subset specification");
            }
        }

        ++a;
    }

    Json::Reader reader;
    Json::Value config;
    reader.parse(configStream, config, false);

    const std::string jsonError(reader.getFormattedErrorMessages());
    if (!jsonError.empty())
    {
        throw std::runtime_error("Config parsing: " + jsonError);
    }

    auto arbiter(getArbiter(credPath));

    // Input files to add to the index.
    const Json::Value jsonInput(config["input"]);
    const std::vector<std::string> manifest(
            getManifest(jsonInput["manifest"], *arbiter));
    const bool trustHeaders(jsonInput["trustHeaders"].asBool());
    const std::size_t runCount(getRunCount(jsonInput, manifest.size()));
    const std::size_t threads(jsonInput["threads"].asUInt64());

    // Build specifications and path info.
    const Json::Value& jsonOutput(config["output"]);
    const std::string outPath(jsonOutput["path"].asString());
    const std::string tmpPath(jsonOutput["tmp"].asString());
    const bool outCompress(jsonOutput["compress"].asUInt64());

    // Tree structure.
    const Json::Value& jsonStructure(config["structure"]);
    const std::size_t nullDepth(jsonStructure["nullDepth"].asUInt64());
    const std::size_t baseDepth(jsonStructure["baseDepth"].asUInt64());
    const std::size_t coldDepth(jsonStructure["coldDepth"].asUInt64());
    const std::size_t chunkPoints(jsonStructure["pointsPerChunk"].asUInt64());
    const std::size_t dynamicChunks(jsonStructure["dynamicChunks"].asBool());
    const std::size_t dimensions(getDimensions(jsonStructure["type"]));
    const std::size_t numPointsHint(
            jsonStructure.isMember("numPointsHint") ?
                jsonStructure["numPointsHint"].asUInt64() : 0);

    const Structure structure(
            nullDepth,
            baseDepth,
            coldDepth,
            chunkPoints,
            dimensions,
            numPointsHint,
            dynamicChunks,
            subset);

    // Geometry and spatial info.
    Json::Value& geometry(config["geometry"]);
    auto bbox(getBBox(geometry["bbox"], dimensions));
    auto reprojection(getReprojection(geometry["reproject"]));
    Schema schema(geometry["schema"]);

    std::unique_ptr<Builder> builder;

    bool exists(false);

    if (!force)
    {
        try
        {
            arbiter::Endpoint endpoint(arbiter->getEndpoint(outPath));
            if (endpoint.getSubpath("entwine").size())
            {
                exists = true;
            }
        }
        catch (...)
        {
            // Hacky...
        }
    }

    if (!force && exists)
    {
        std::cout << "Continuing previous index..." << std::endl;
        std::cout <<
            "\nInput:\n" <<
            "\tBuilding from " << manifest.size() << " source files\n" <<
            "\tInserting up to " << runCount << " files\n" <<
            "\tBuild threads: " << threads << "\n" <<
            "Output:\n" <<
            "\tContinuing from output path: " << outPath << "\n" <<
            "\tTemporary path: " << tmpPath << "\n" <<
            "\tCompressed output: (inferring from previous build)\n" <<
            "Tree structure:\n" <<
            "\t(inferring from previous build)\n" <<
            "Geometry:\n" <<
            "\tBounds: (inferring from previous build)\n" <<
            "\tReprojection: " << getReprojString(reprojection.get()) << "\n" <<
            "\tStoring dimensions: (inferring from previous build)\n" <<
            std::endl;

        builder.reset(new Builder(outPath, tmpPath, threads, arbiter));
    }
    else
    {
        if (!bbox && manifest.size() > 1)
        {
            throw std::runtime_error(
                    "Can't infer bounds from multiple sources");
        }

        std::cout <<
            "\nInput:\n" <<
            "\tBuilding from " << manifest.size() << " source files\n" <<
            "\tInserting up to " << runCount << " files\n" <<
            "\tTrust file headers? " << (trustHeaders ? "Yes" : "No") << "\n" <<
            "\tBuild threads: " << threads << "\n" <<
            "Output:\n" <<
            "\tOutput path: " << outPath << "\n" <<
            "\tTemporary path: " << tmpPath << "\n" <<
            "\tCompressed output? " << (outCompress ? "Yes" : "No") << "\n" <<
            "Tree structure:\n" <<
            "\tNull depth: " << nullDepth << "\n" <<
            "\tBase depth: " << baseDepth << "\n" <<
            "\tCold depth: " << coldDepth << "\n" <<
            "\tChunk size: " << chunkPoints << " points\n" <<
            "\tDynamic chunks? " << (dynamicChunks ? "Yes" : "No") << "\n" <<
            "\tBuild type: " << jsonStructure["type"].asString() << "\n" <<
            "\tPoint count hint: " << numPointsHint << " points\n" <<
            "Geometry:\n" <<
            "\tBounds: " << *bbox << "\n" <<
            "\tReprojection: " << getReprojString(reprojection.get()) << "\n" <<
            "\tStoring dimensions: " << getDimensionString(schema) << "\n" <<
            std::endl;

        if (structure.isSubset())
        {
            std::cout << "Subset: " <<
                subset.first + 1 << " of " << subset.second << "\n" <<
                std::endl;
        }

        builder.reset(
                new Builder(
                    outPath,
                    tmpPath,
                    trustHeaders,
                    reprojection.get(),
                    bbox.get(),
                    schema.dims(),
                    threads,
                    structure,
                    arbiter));
    }

    // Index.
    std::size_t i(0);
    std::size_t numInserted(0);

    auto start = now();

    while (i < manifest.size() && (numInserted < runCount || !runCount))
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


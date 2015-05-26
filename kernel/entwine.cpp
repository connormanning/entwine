/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <fstream>
#include <iomanip>
#include <iostream>

#include <csignal>
#include <cstdio>
#include <execinfo.h>
#include <string>

#include <entwine/drivers/arbiter.hpp>
#include <entwine/drivers/s3.hpp>
#include <entwine/drivers/source.hpp>
#include <entwine/third/json/json.h>
#include <entwine/tree/builder.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/fs.hpp>

using namespace entwine;

namespace
{
    Json::Reader reader;

    void handler(int sig) {
        void* array[16];
        const std::size_t size(backtrace(array, 16));

        std::cout << "Got error " << sig << std::endl;
        backtrace_symbols_fd(array, size, STDERR_FILENO);
        exit(1);
    }

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

    std::string getBBoxString(const BBox* bbox)
    {
        std::ostringstream oss;

        if (bbox)
        {
            oss << "[(" << std::fixed <<
                bbox->min().x << ", " << bbox->min().y << "), (" <<
                bbox->max().x << ", " << bbox->max().y << ")]";
        }
        else
        {
            oss << "(Inferring from source)";
        }

        return oss.str();
    }

    std::unique_ptr<AwsAuth> getCredentials(const std::string credPath)
    {
        std::unique_ptr<AwsAuth> auth;

        Json::Value credentials;
        std::ifstream credFile(credPath, std::ifstream::binary);
        if (credFile.good())
        {
            reader.parse(credFile, credentials, false);

            auth.reset(
                    new AwsAuth(
                        credentials["access"].asString(),
                        credentials["hidden"].asString()));
        }

        return auth;
    }

    std::vector<std::string> getManifest(const Json::Value& json)
    {
        std::vector<std::string> manifest;

        if (json.isArray())
        {
            manifest.resize(json.size());

            for (Json::ArrayIndex i(0); i < json.size(); ++i)
            {
                manifest[i] = json[i].asString();
            }
        }
        else
        {
            manifest.push_back(json.asString());
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

    std::unique_ptr<BBox> getBBox(const Json::Value& json)
    {
        std::unique_ptr<BBox> bbox;

        if (json.isArray())
        {
            bbox.reset(new BBox(json));
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
}

int main(int argc, char** argv)
{
    signal(SIGSEGV, handler);

    if (argc < 2)
    {
        std::cout << "Input file required." << std::endl <<
            "\tUsage: entwine <config> [options]" << std::endl;
        exit(1);
    }

    const std::string configFilename(argv[1]);
    std::ifstream configStream(configFilename, std::ifstream::binary);
    if (!configStream.good())
    {
        std::cout << "Couldn't open " << configFilename << " for reading." <<
            std::endl;
        exit(1);
    }

    std::string credPath("credentials.json");
    bool force(false);
    std::pair<std::size_t, std::size_t> subset({ 0, 0 });

    int argNum(2);

    while (argNum < argc)
    {
        std::string arg(argv[argNum]);
        ++argNum;

        if (arg == "-c")
        {
            credPath = argv[argNum++];
        }
        else if (arg.size() > 2 && arg.substr(0, 2) == "-c")
        {
            credPath = arg.substr(2);
        }
        else if (arg == "-f")
        {
            force = true;
        }
        else if (arg == "-s")
        {
            if (argNum + 1 < argc)
            {
                subset = {
                    std::stoul(argv[argNum]) - 1,
                    std::stoul(argv[argNum + 1]) };

                argNum += 2;

                if (subset.first >= subset.second)
                    throw std::runtime_error("Invalid subset parameters.");
            }
            else
            {
                throw std::runtime_error("Invalid subset parameters.");
            }
        }
    }

    Json::Value config;
    reader.parse(configStream, config, false);

    // Input files to add to the index.
    const Json::Value jsonInput(config["input"]);
    const std::vector<std::string> manifest(getManifest(jsonInput["manifest"]));
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
            subset);

    // Geometry and spatial info.
    const Json::Value& geometry(config["geometry"]);
    auto bbox(getBBox(geometry["bbox"]));
    auto reprojection(getReprojection(geometry["reproject"]));
    Schema schema(geometry["schema"]);

    DriverMap drivers;

    {
        std::unique_ptr<AwsAuth> auth(getCredentials(credPath));
        if (auth)
        {
            drivers.insert({ "s3", std::make_shared<S3Driver>(*auth) });
        }
    }

    std::shared_ptr<Arbiter> arbiter(std::make_shared<Arbiter>(drivers));

    std::unique_ptr<Builder> builder;

    // TODO Fetch from arbiter source - don't assume local.
    if (!force && fs::fileExists(outPath + "/entwine"))
    {
        std::cout << "Continuing previous index..." << std::endl;
        std::cout <<
            "Input:\n" <<
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
            std::cout << "Can't infer bounds from multiple inputs" << std::endl;
            exit(1);
        }

        std::cout <<
            "Input:\n" <<
            "\tBuilding from " << manifest.size() << " source files\n" <<
            "\tInserting up to " << runCount << " files\n" <<
            "\tBuild threads: " << threads << "\n" <<
            "Output:\n" <<
            "\tOutput path: " << outPath << "\n" <<
            "\tTemporary path: " << tmpPath << "\n" <<
            "\tCompressed output: " << std::boolalpha << outCompress << "\n" <<
            "Tree structure:\n" <<
            "\tNull depth: " << nullDepth << "\n" <<
            "\tBase depth: " << baseDepth << "\n" <<
            "\tCold depth: " << coldDepth << "\n" <<
            "\tChunk size: " << chunkPoints << " points\n" <<
            "\tBuild type: " << jsonStructure["type"].asString() << "\n" <<
            "\tPoint count hint: " << numPointsHint << " points\n" <<
            "Geometry:\n" <<
            "\tBounds: " << getBBoxString(bbox.get()) << "\n" <<
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


    return 0;
}


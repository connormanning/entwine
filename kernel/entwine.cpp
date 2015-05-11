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

    std::string getDimensionString(DimList dims)
    {
        std::string results("[");

        for (std::size_t i(0); i < dims.size(); ++i)
        {
            if (i) results += ", ";
            results += dims[i].name();
        }

        results += "]";

        return results;
    }

    std::string getBBoxString(const BBox& bbox)
    {
        std::ostringstream oss;

        oss << "[(" <<
            bbox.min().x << ", " << bbox.min().y << "), (" <<
            bbox.max().x << ", " << bbox.max().y << ")]";

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

    std::vector<std::string> getManifest(const Json::Value& jsonInput)
    {
        std::vector<std::string> manifest(jsonInput.size());

        for (Json::ArrayIndex i(0); i < jsonInput.size(); ++i)
        {
            manifest[i] = jsonInput[i].asString();
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
    if (argc == 4)
    {
        if (std::string(argv[2]) == "-c")
        {
            credPath = argv[3];
        }
    }

    Json::Value config;
    reader.parse(configStream, config, false);

    // Input files to add to the index.
    const Json::Value input(config["input"]);
    const std::vector<std::string> manifest(getManifest(input["manifest"]));
    const std::size_t runCount(getRunCount(input, manifest.size()));

    // Build specifications and path info.
    const Json::Value& build(config["build"]);
    const std::string buildPath(build["path"].asString());
    const std::string tmpPath(build["tmp"].asString());
    const std::size_t threads(build["threads"].asUInt64());

    // Tree structure.
    const Json::Value& tree(build["tree"]);
    const std::size_t buildChunkPoints(tree["pointsPerChunk"].asUInt64());
    const std::size_t baseDepth(tree["baseDepth"].asUInt64());
    const std::size_t flatDepth(0);
    const std::size_t coldDepth(tree["coldDepth"].asUInt64());

    // Output info.
    const Json::Value& output(config["output"]);
    const std::string exportPath(output["path"].asString());
    const std::size_t exportChunkPoints(output["pointsPerChunk"].asUInt64());
    const std::size_t exportBase(output["baseDepth"].asUInt64());
    const bool exportCompress(output["compress"].asBool());

    // Geometry and spatial info.
    const Json::Value& geometry(config["geometry"]);
    const std::size_t dimensions(getDimensions(geometry["type"]));
    const BBox bbox(BBox::fromJson(geometry["bbox"]));
    std::unique_ptr<Reprojection> reprojection(
            getReprojection(geometry["reproject"]));
    DimList dims(Schema::fromJson(geometry["schema"]));

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

    if (fs::fileExists(buildPath + "/meta"))
    {
        std::cout << "Continuing previous index..." << std::endl;
        std::cout << "Paths:\n" <<
            "\tBuilding from " << manifest.size() << " source files" << "\n" <<
            "\tInserting up to " << runCount << " files\n" <<
            "\tBuild path: " << buildPath << "\n" <<
            "\tTmp path: " << tmpPath << "\n" <<
            "\tBuild threads:  " << threads << "\n" << std::endl;

        builder.reset(
                new Builder(
                    buildPath,
                    tmpPath,
                    reprojection.get(),
                    threads,
                    arbiter));
    }
    else
    {
        std::cout << "Paths:\n" <<
            "\tBuilding from " << manifest.size() << " source files\n" <<
            "\tInserting up to " << runCount << " files\n" <<
            "\tBuild path: " << buildPath << "\n" <<
            "\tBuild threads: " << threads << "\n" <<
            "\tBuild tree: " << "\n" <<
            "\t\tChunk size: " << buildChunkPoints << " points\n" <<
            "\t\tBase depth: " << baseDepth << "\n" <<
            // "\t\tFlat depth: " << flatDepth << "\n" <<
            "\t\tCold depth: " << coldDepth << "\n" <<
            "\tTmp path: " << tmpPath << "\n" <<
            "\tOutput path: " << exportPath << "\n" <<
            "\t\tExport chunk size: " << exportChunkPoints << " points\n" <<
            "\t\tExport base depth: " << exportBase << std::endl;
        std::cout << "Geometry:\n" <<
            "\tBuild type: " << geometry["type"].asString() << "\n" <<
            "\tBounds: " << getBBoxString(bbox) << "\n" <<
            "\tReprojection: " << getReprojString(reprojection.get()) << "\n" <<
            "\tStoring dimensions: " << getDimensionString(dims) << std::endl;

        builder.reset(
                new Builder(
                    buildPath,
                    tmpPath,
                    reprojection.get(),
                    bbox,
                    dims,
                    threads,
                    dimensions,
                    buildChunkPoints,
                    baseDepth,
                    flatDepth,
                    coldDepth,
                    arbiter));
    }

    const auto start(std::chrono::high_resolution_clock::now());

    std::size_t i(0);
    std::size_t numInserted(0);

    while (i < manifest.size() && (numInserted < runCount || !runCount))
    {
        if (builder->insert(manifest[i++]))
        {
            ++numInserted;
        }
    }

    builder->save();

    const auto end(std::chrono::high_resolution_clock::now());
    const std::chrono::duration<double> d(end - start);
    std::cout << "Index/save completed in " <<
            std::chrono::duration_cast<std::chrono::seconds>(d).count() <<
            " seconds\n" << std::endl;

    std::cout << "Exporting..." << std::endl;
    builder->finalize(
            exportPath,
            exportChunkPoints,
            exportBase,
            exportCompress);

    std::cout << "Finished." << std::endl;
    return 0;
}


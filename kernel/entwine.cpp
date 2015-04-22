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
#include <iostream>

#include <csignal>
#include <cstdio>
#include <execinfo.h>
#include <string>

#include <entwine/http/s3.hpp>
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
        void *array[16];
        size_t size;

        // get void*'s for all entries on the stack
        size = backtrace(array, 16);

        // print out all the frames to stderr
        std::cout << "Got error " << sig << std::endl;
        backtrace_symbols_fd(array, size, STDERR_FILENO);
        exit(1);
    }

    std::string getDimensionString(DimList dims)
    {
        std::string results("[");

        for (std::size_t i(0); i < dims.size(); ++i)
        {
            results += dims[i].name();
            if (i < dims.size() - 1) std::cout << ", ";
        }

        results += "]";

        return results;
    }

    S3Info getCredentials()
    {
        // TODO Accept flag to specify location of credentials file.
        Json::Value credentials;
        std::ifstream credFile("credentials.json", std::ifstream::binary);
        if (credFile.good())
        {
            reader.parse(credFile, credentials, false);
            return S3Info(
                    credentials["url"].asString(),
                    credentials["bucket"].asString(),
                    credentials["access"].asString(),
                    credentials["hidden"].asString());
        }
        else
        {
            return S3Info();
        }
    }

    std::vector<std::string> getInput(const Json::Value& jsonInput)
    {
        std::vector<std::string> input(jsonInput.size());

        for (Json::ArrayIndex i(0); i < jsonInput.size(); ++i)
        {
            input[i] = jsonInput[i].asString();
        }

        return input;
    }

    std::size_t getDimensions(const Json::Value& jsonType)
    {
        const std::string typeString(jsonType.asString());

        if (typeString == "quadtree") return 2;
        else if (typeString == "octree") return 3;
        else throw std::runtime_error("Invalid tree type");
    }

    Reprojection getReprojection(const Json::Value& jsonReproject)
    {
        Reprojection reprojection;

        if (jsonReproject.isMember("in") && jsonReproject.isMember("out"))
        {
            reprojection = Reprojection(
                    jsonReproject["in"].asString(),
                    jsonReproject["out"].asString());
        }

        return reprojection;
    }

    std::string getReprojectionString(Reprojection reprojection)
    {
        if (reprojection.valid())
        {
            return reprojection.in() + " -> " + reprojection.out();
        }
        else
        {
            return "none";
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

    Json::Value config;
    reader.parse(configStream, config, false);

    // Input files to add to the index.
    const std::vector<std::string> input(getInput(config["input"]));

    // Build specifications and path info.
    const Json::Value& build(config["build"]);
    const std::string buildPath(build["path"].asString());
    const std::string tmpPath(build["tmp"].asString());

    const Json::Value& tree(build["tree"]);
    const std::size_t baseDepth(tree["baseDepth"].asUInt64());
    const std::size_t flatDepth(tree["flatDepth"].asUInt64());
    const std::size_t diskDepth(tree["diskDepth"].asUInt64());

    // Output info.
    const Json::Value& output(config["output"]);
    const std::string exportPath(output["export"].asString());
    const std::size_t exportBase(output["baseDepth"].asUInt64());
    const bool exportCompress(output["compress"].asBool());

    // Performance tuning.
    const Json::Value& tuning(config["tuning"]);
    const std::size_t snapshot(tuning["snapshot"].asUInt64());
    const std::size_t threads(tuning["threads"].asUInt64());

    // Geometry and spatial info.
    const Json::Value& geometry(config["geometry"]);
    const std::size_t dimensions(getDimensions(geometry["type"]));
    const BBox bbox(BBox::fromJson(geometry["bbox"]));
    const Reprojection reprojection(getReprojection(geometry["reproject"]));
    DimList dims(Schema::fromJson(geometry["schema"]));

    const S3Info s3Info(getCredentials());

    std::unique_ptr<Builder> builder;

    if (fs::fileExists(buildPath + "/meta"))
    {
        std::cout << "Continuing previous index..." << std::endl;
        std::cout << "Paths:\n" <<
            "\tBuilding from " << input.size() << " source files" << "\n" <<
            "\tBuild path: " << buildPath << "\n" <<
            "\tTmp path: " << tmpPath << std::endl;
        std::cout << "Performance tuning:\n" <<
            "\tSnapshot: " << snapshot << "\n" <<
            "\tThreads:  " << threads << std::endl;

        builder.reset(
                new Builder(
                    buildPath,
                    tmpPath,
                    reprojection,
                    s3Info,
                    threads));
    }
    else
    {
        std::cout << "Paths:\n" <<
            "\tBuilding from " << input.size() << " source files" << "\n" <<
            "\tBuild path: " << buildPath << "\n" <<
            "\t\tBuild tree: " << "\n" <<
            "\t\t\tBase depth: " << baseDepth << "\n" <<
            "\t\t\tFlat depth: " << flatDepth << "\n" <<
            "\t\t\tDisk depth: " << diskDepth << "\n" <<
            "\tTmp path: " << tmpPath << "\n" <<
            "\tOutput path: " << exportPath << "\n" <<
            "\t\tExport base depth: " << exportBase << std::endl;
        std::cout << "Geometry:\n" <<
            "\tBuild type: " << geometry["type"].asString() << "\n" <<
            "\tBounds: " << bbox.toJson().toStyledString() << "\n" <<
            "\tReprojection: " << getReprojectionString(reprojection) << "\n" <<
            "\tStoring dimensions: " << getDimensionString(dims) << std::endl;
        std::cout << "Performance tuning:\n" <<
            "\tSnapshot: " << snapshot << "\n" <<
            "\tThreads: " << threads << std::endl;

        builder.reset(
                new Builder(
                    buildPath,
                    tmpPath,
                    reprojection,
                    bbox,
                    dims,
                    s3Info,
                    threads,
                    dimensions,
                    baseDepth,
                    flatDepth,
                    diskDepth));
    }

    const auto start(std::chrono::high_resolution_clock::now());
    for (std::size_t i(0); i < input.size(); ++i)
    {
        builder->insert(input[i]);

        if (snapshot && ((i + 1) % snapshot) == 0)
        {
            builder->save();
        }
    }

    builder->join();

    const auto end(std::chrono::high_resolution_clock::now());
    const std::chrono::duration<double> d(end - start);
    std::cout << "Indexing complete - " <<
            std::chrono::duration_cast<std::chrono::seconds>(d).count() <<
            " seconds\n" << "Saving to disk..." << std::endl;

    builder->save();
    std::cout << "Done.  Exporting..." << std::endl;

    // TODO For now only S3 export supported.
    S3Info exportInfo(
            s3Info.baseAwsUrl,
            exportPath,
            s3Info.awsAccessKeyId,
            s3Info.awsSecretAccessKey);

    builder->finalize(exportInfo, exportBase, exportCompress);
    std::cout << "Finished." << std::endl;

    return 0;
}


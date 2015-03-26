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

#include <entwine/third/json/json.h>
#include <entwine/tree/multi-batcher.hpp>
#include <entwine/tree/sleepy-tree.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/fs.hpp>

namespace
{
    Json::Reader reader;
}

using namespace entwine;

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
        // TODO Just until other data sources are supported.
        std::cout << "S3 credentials not found - credentials.json" << std::endl;
        exit(1);
    }
}

std::vector<std::string> getPaths(const Json::Value& jsonPaths)
{
    // TODO Support local and HTTP paths.
    std::vector<std::string> paths(jsonPaths["s3"].size());
    for (std::size_t i(0); i < jsonPaths["s3"].size(); ++i)
    {
        paths[i] = jsonPaths["s3"][static_cast<Json::ArrayIndex>(i)].asString();
    }

    return paths;
}

int main(int argc, char** argv)
{
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

    // Parse configuration.
    const std::vector<std::string> paths(getPaths(config["manifest"]));
    const BBox bbox(BBox::fromJson(config["bbox"]));
    Schema schema(Schema::fromJson(config["schema"]));
    const S3Info s3Info(getCredentials());

    // TODO These values should be overridable via command line.
    const std::string outDir(config["output"].asString());
    const std::size_t dimensions(config["dimensions"].asUInt64());
    const Json::Value& tuning(config["tuning"]);
    const std::size_t snapshot(tuning["snapshot"].asUInt64());
    const std::size_t threads(tuning["threads"].asUInt64());

    const Json::Value& tree(config["tree"]);
    const std::size_t baseDepth(tree["baseDepth"].asUInt64());
    const std::size_t flatDepth(tree["flatDepth"].asUInt64());
    const std::size_t diskDepth(tree["diskDepth"].asUInt64());

    if (!fs::mkdirp(outDir))
    {
        std::cout << "Could not create output dir: " << outDir << std::endl;
        return 1;
    }

    std::cout << "Building from " << paths.size() << " paths." << std::endl;
    std::cout << "Storing dimensions: [";
    for (std::size_t i(0); i < schema.dims().size(); ++i)
    {
        std::cout << schema.dims()[i].name();
        if (i < schema.dims().size() - 1) std::cout << ", ";
    }
    std::cout << "]" << std::endl;
    std::cout << "S3 source: " << s3Info.baseAwsUrl <<
        "/" << s3Info.bucketName << std::endl;
    std::cout << "Performance tuning:\n" <<
        "\tSnapshot: " << snapshot << "\n" <<
        "\tThreads:  " << threads << std::endl;
    std::cout << "Saving to: " << outDir << std::endl;
    std::cout << "BBox: " << bbox.toJson().toStyledString() << std::endl;

    std::unique_ptr<SleepyTree> sleepyTree(
            new SleepyTree(
                outDir,
                bbox,
                schema,
                dimensions,
                baseDepth,
                flatDepth,
                diskDepth));

    MultiBatcher batcher(
            s3Info,
            *sleepyTree.get(),
            threads,
            snapshot);

    const auto start(std::chrono::high_resolution_clock::now());
    for (const auto& path : paths)
    {
        batcher.add(path);
    }

    batcher.gather();
    const auto end(std::chrono::high_resolution_clock::now());
    const std::chrono::duration<double> d(end - start);
    std::cout << "Indexing complete - " <<
            std::chrono::duration_cast<std::chrono::seconds>(d).count() <<
            " seconds\n" << "Saving to disk..." << std::endl;

    sleepyTree->save();
    std::cout << "Save complete." << std::endl;

    return 0;
}


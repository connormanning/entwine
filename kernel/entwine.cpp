/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <iostream>

#include "json/json.h"
#include "tree/multi-batcher.hpp"
#include "tree/sleepy-tree.hpp"
#include "types/bbox.hpp"
#include "types/schema.hpp"

namespace
{
    Json::Reader reader;
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
    const S3Info s3Info(getCredentials());

    // TODO These values should be overridable via command line.
    const std::string outDir(config["output"].asString());
    const Json::Value& tuning(config["tuning"]);
    const std::size_t snapshot(tuning["snapshot"].asUInt64());
    const std::size_t threads(tuning["threads"].asUInt64());

    const std::size_t baseDepth(config["baseDepth"].asUInt64());
    const std::size_t flatDepth(config["flatDepth"].asUInt64());
    const std::size_t diskDepth(config["diskDepth"].asUInt64());

    // Parse resulting schema.
    Schema schema(Schema::fromJson(config["schema"]));

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
                baseDepth,
                flatDepth,
                diskDepth));

    MultiBatcher batcher(s3Info, threads, *sleepyTree.get());
    const auto start(std::chrono::high_resolution_clock::now());
    for (std::size_t i(0); i < paths.size(); ++i)
    {
        batcher.add(paths[i], i);

        if ((i + 1) % snapshot == 0)
        {
            std::cout <<
                "Gathering responses for restore point at " <<
                i + 1 << " / " << paths.size() <<
                std::endl;
            batcher.gather();
            std::cout << "Creating restore" << std::endl;

            std::ofstream dataStream;
            dataStream.open(
                    outDir + "/meta",
                    std::ofstream::out | std::ofstream::trunc);
            std::string info(std::to_string(i) + "," + paths[i]);
            dataStream.write(info.data(), info.size());
            dataStream.close();

            sleepyTree->save();
            std::cout << "Restore point created" << std::endl;
        }
    }

    batcher.gather();
    const auto end(std::chrono::high_resolution_clock::now());
    const std::chrono::duration<double> d(end - start);
    std::cout << "Indexing complete - " <<
            std::chrono::duration_cast<std::chrono::seconds>(d).count() <<
            " seconds\n" << "Saving to disk..." << std::endl;

    sleepyTree->save();
    std::cout << "Save complete." << std::endl;
}


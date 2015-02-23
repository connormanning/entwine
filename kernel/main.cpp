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
#include "types/schema.hpp"

namespace
{
    Json::Reader reader;
}

S3Info getCredentials()
{
    Json::Value credentials;
    std::ifstream credFile("credentials.json", std::ifstream::binary);
    reader.parse(credFile, credentials, false);
    std::cout << credentials.toStyledString() << std::endl;
    return S3Info(
            credentials["url"].asString(),
            credentials["bucket"].asString(),
            credentials["access"].asString(),
            credentials["hidden"].asString());
}

std::vector<std::string> getPaths(const Json::Value& jsonPaths)
{
    std::vector<std::string> paths(jsonPaths["s3"].size());
    for (std::size_t i(0); i < jsonPaths["s3"].size(); ++i)
    {
        paths[i] = jsonPaths["s3"][static_cast<Json::ArrayIndex>(i)].asString();
    }

    std::cout << "Found " << paths.size() << " paths." << std::endl;

    return paths;
}

int main(int argc, char** argv)
{
    if (argc < 2)
    {
        std::cout << "Input file required." << std::endl <<
            "\tUsage: entwine config.json [options]" << std::endl;
        exit(1);
    }

    const std::string runFile(argv[1]);
    std::ifstream runStream(runFile, std::ifstream::binary);
    if (!runStream.good())
    {
        std::cout << "Couldn't open " << runFile << " for reading." <<
            std::endl;
        exit(1);
    }

    Json::Value config;
    reader.parse(runStream, config, false);

    const std::vector<std::string> paths(getPaths(config["manifest"]));
    const BBox bbox(BBox::fromJson(config["bbox"]));

    // TODO Construct from config.
    std::vector<DimInfo> dims(Schema::dimInfoList(config["schema"]));
    Schema schema(dims);

    std::shared_ptr<SleepyTree> sleepyTree(
            new SleepyTree(
                "./out",
                bbox,
                schema));

    S3Info s3Info(getCredentials());

    std::size_t numBatches(2); // TODO
    MultiBatcher batcher(s3Info, "./out", numBatches, sleepyTree);
    const auto start(std::chrono::high_resolution_clock::now());
    for (std::size_t i(0); i < paths.size(); ++i)
    {
        batcher.add(paths[i], i);

        /*
        if ((i + 1) % restorePoint == 0)
        {
            std::cout <<
                "Gathering responses for restore point at " <<
                i + 1 << " / " << paths.size() <<
                std::endl;
            batcher.gather();
            std::cout << "Creating restore" << std::endl;

            // TODO Hardcoded path.
            std::ofstream dataStream;
            dataStream.open(
                    "/var/greyhound/serial/" + pipelineId + "/restore-info",
                    std::ofstream::out | std::ofstream::trunc);
            std::string info(std::to_string(i) + "," + paths[i]);
            dataStream.write(info.data(), info.size());
            dataStream.close();

            sleepyTree->save(
                    "/var/greyhound/serial/" + pipelineId + "/restore");
            std::cout << "Restore point created" << std::endl;
        }
        */
    }

    batcher.gather();
    const auto end(std::chrono::high_resolution_clock::now());
    const std::chrono::duration<double> d(end - start);
    std::cout << "Multi " << /*pipelineId << */" complete - took " <<
            std::chrono::duration_cast<std::chrono::seconds>(d).count() <<
            " seconds" <<
            std::endl;

    sleepyTree->save();
}


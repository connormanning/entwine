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

#include <entwine/http/s3.hpp>
#include <entwine/third/json/json.h>
#include <entwine/tree/sleepy-tree.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/fs.hpp>

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

std::vector<std::string> getManifest(const Json::Value& jsonManifest)
{
    // TODO Support local and HTTP manifest.
    std::vector<std::string> manifest(jsonManifest["s3"].size());

    for (Json::ArrayIndex i(0); i < jsonManifest["s3"].size(); ++i)
    {
        manifest[i] = jsonManifest["s3"][i].asString();
    }

    return manifest;
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

    // TODO Some of these values should be overridable via command line.

    // Parse configuration.
    const std::vector<std::string> manifest(getManifest(config["manifest"]));
    const BBox bbox(BBox::fromJson(config["bbox"]));
    DimList dimList(Schema::fromJson(config["schema"]));
    const S3Info s3Info(getCredentials());

    const Json::Value& paths(config["paths"]);
    const std::string buildDir(paths["build"].asString());
    const std::string exportPath(paths["export"].asString());
    const std::size_t exportBase(paths["baseDepth"].asUInt64());
    const bool exportCompress(paths["compress"].asBool());

    const std::string finalStore(config["export"].asString());
    const std::size_t dimensions(config["dimensions"].asUInt64());
    const Json::Value& tuning(config["tuning"]);
    const std::size_t snapshot(tuning["snapshot"].asUInt64());
    const std::size_t threads(tuning["threads"].asUInt64());

    const Json::Value& tree(config["tree"]);
    const std::size_t baseDepth(tree["baseDepth"].asUInt64());
    const std::size_t flatDepth(tree["flatDepth"].asUInt64());
    const std::size_t diskDepth(tree["diskDepth"].asUInt64());

    if (!fs::mkdirp(buildDir))
    {
        std::cout << "Could not create output dir: " << buildDir << std::endl;
        return 1;
    }

    std::unique_ptr<SleepyTree> sleepyTree;

    std::cout << "Building from " << manifest.size() << " paths." << std::endl;

    if (fs::fileExists(buildDir + "/meta"))
    {
        // Adding to a previous build.
        sleepyTree.reset(new SleepyTree(buildDir, s3Info, threads));
    }
    else
    {
        std::cout << "Storing dimensions: [";
        for (std::size_t i(0); i < dimList.size(); ++i)
        {
            std::cout << dimList[i].name();
            if (i < dimList.size() - 1) std::cout << ", ";
        }
        std::cout << "]" << std::endl;
        std::cout << "S3 source: " << s3Info.baseAwsUrl <<
            "/" << s3Info.bucketName << std::endl;
        std::cout << "Performance tuning:\n" <<
            "\tSnapshot: " << snapshot << "\n" <<
            "\tThreads:  " << threads << std::endl;
        std::cout << "Saving to: " << buildDir << std::endl;
        std::cout << "BBox: " << bbox.toJson().toStyledString() << std::endl;

        sleepyTree.reset(
                new SleepyTree(
                    buildDir,
                    bbox,
                    dimList,
                    s3Info,
                    threads,
                    dimensions,
                    baseDepth,
                    flatDepth,
                    diskDepth));
    }

    const auto start(std::chrono::high_resolution_clock::now());
    for (std::size_t i(0); i < manifest.size(); ++i)
    {
        sleepyTree->insert(manifest[i]);

        if (snapshot && ((i + 1) % snapshot) == 0)
        {
            sleepyTree->save();
        }
    }

    sleepyTree->join();

    const auto end(std::chrono::high_resolution_clock::now());
    const std::chrono::duration<double> d(end - start);
    std::cout << "Indexing complete - " <<
            std::chrono::duration_cast<std::chrono::seconds>(d).count() <<
            " seconds\n" << "Saving to disk..." << std::endl;

    sleepyTree->save();
    std::cout << "Done.  Exporting..." << std::endl;

    // TODO For now only S3 export supported.
    S3Info exportInfo(
            s3Info.baseAwsUrl,
            exportPath,
            s3Info.awsAccessKeyId,
            s3Info.awsSecretAccessKey);

    sleepyTree->finalize(exportInfo, exportBase, exportCompress);
    std::cout << "Finished." << std::endl;

    return 0;
}


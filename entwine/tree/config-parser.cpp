/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <cmath>
#include <limits>
#include <numeric>

#include <entwine/tree/config-parser.hpp>

#include <entwine/formats/cesium/settings.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/inference.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/storage.hpp>
#include <entwine/types/manifest.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/env.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    const bool shallow(
            env("TESTING_SHALLOW") &&
            *env("TESTING_SHALLOW") == "true");
}

namespace
{
    Json::Reader reader;

    std::unique_ptr<cesium::Settings> getCesiumSettings(const Json::Value& json)
    {
        std::unique_ptr<cesium::Settings> settings;

        if (json.isMember("cesium"))
        {
            settings = makeUnique<cesium::Settings>(json["cesium"]);
        }

        return settings;
    }
}

Json::Value ConfigParser::defaults()
{
    Json::Value json;

    json["input"] = Json::Value::null;
    json["output"] = Json::Value::null;
    json["tmp"] = arbiter::fs::getTempPath();
    json["threads"] = 8;
    json["trustHeaders"] = true;
    json["prefixIds"] = false;
    json["storage"] = "laszip";

    if (!shallow)
    {
        json["pointsPerChunk"] = std::pow(4, 9);
        json["nullDepth"] = 7;
        json["baseDepth"] = 10;
    }
    else
    {
        std::cout << "Using shallow test configuration" << std::endl;

        json["pointsPerChunk"] = std::pow(4, 5);
        json["nullDepth"] = 4;
        json["baseDepth"] = 6;
    }

    return json;
}

std::unique_ptr<Builder> ConfigParser::getBuilder(
        Json::Value json,
        std::shared_ptr<arbiter::Arbiter> arbiter)
{
    if (!arbiter) arbiter = std::make_shared<arbiter::Arbiter>();

    const bool verbose(json["verbose"].asBool());

    const Json::Value d(defaults());
    for (const auto& k : d.getMemberNames())
    {
        if (!json.isMember(k)) json[k] = d[k];
    }

    const std::string out(json["output"].asString());
    const std::string tmp(json["tmp"].asString());
    const std::size_t threads(json["threads"].asUInt64());

    const auto outType(arbiter::Arbiter::getType(out));
    if (outType == "s3" || outType == "gs") json["prefixIds"] = true;

    normalizeInput(json, *arbiter);
    auto fileInfo(extract<FileInfo>(json["input"]));

    if (!json["force"].asBool())
    {
        if (auto builder = tryGetExisting(json, arbiter, out, tmp, threads))
        {
            if (verbose)
            {
                builder->verbose(true);
                std::cout << "Scanning for new files..." << std::endl;
            }

            // Only scan for files that aren't already in the index.
            fileInfo = builder->metadata().manifest().diff(fileInfo);

            if (fileInfo.size())
            {
                Inference inference(*builder, fileInfo);
                inference.go();
                fileInfo = inference.fileInfo();

                std::cout << "Adding " << fileInfo.size() << " new files" <<
                    std::endl;
            }

            // If we have more paths to add, add them to the manifest.
            // Otherwise we might be continuing a partial build, in which case
            // the paths to be built are already outstanding in the manifest.
            //
            // It's plausible that the input field could be empty to continue
            // a previous build.
            if (json["input"].isArray()) builder->append(fileInfo);
            return builder;
        }
    }

    if (json["absolute"].asBool() && json["storage"].asString() == "laszip")
    {
        json["storage"] = "lazperf";
    }

    const auto storage(toChunkStorageType(json["storage"]));
    const bool trustHeaders(json["trustHeaders"].asBool());
    const bool storePointId(json["storePointId"].asBool());
    auto cesiumSettings(getCesiumSettings(json["formats"]));
    bool absolute(json["absolute"].asBool());

    if (cesiumSettings)
    {
        json["reprojection"]["out"] = "EPSG:4978";
    }

    auto reprojection(maybeCreate<Reprojection>(json["reprojection"]));

    std::unique_ptr<std::vector<double>> transformation;
    std::unique_ptr<Delta> delta;
    if (!absolute && Delta::existsIn(json)) delta = makeUnique<Delta>(json);

    // If we're building from an inference, then we already have these.  A user
    // could have also pre-supplied them in the config.
    //
    // Either way, these three values are prerequisites for building, so if
    // we're missing any we'll need to infer them from the files.
    std::size_t numPointsHint(json["numPointsHint"].asUInt64());
    auto boundsConforming(maybeCreate<Bounds>(json["bounds"]));
    auto schema(maybeCreate<Schema>(json["schema"]));

    if (json.isMember("transformation"))
    {
        transformation = makeUnique<std::vector<double>>(
                extract<double>(json["transformation"]));
    }

    const bool needsInference(!boundsConforming || !schema || !numPointsHint);

    if (needsInference)
    {
        if (verbose)
        {
            std::cout << "Performing dataset inference..." << std::endl;
        }

        Inference inference(
                fileInfo,
                reprojection.get(),
                trustHeaders,
                !absolute,
                tmp,
                threads,
                verbose,
                !!cesiumSettings,
                arbiter.get());

        if (transformation)
        {
            inference.transformation(*transformation);
        }

        inference.go();

        // Overwrite our initial fileInfo with the inferred version, which
        // contains details for each file instead of just paths.
        fileInfo = inference.fileInfo();

        if (!absolute && inference.delta())
        {
            if (!delta) delta = makeUnique<Delta>();

            if (!json.isMember("scale"))
            {
                delta->scale() = inference.delta()->scale();
            }

            if (!json.isMember("offset"))
            {
                delta->offset() = inference.delta()->offset();
            }
        }

        if (!boundsConforming)
        {
            boundsConforming = makeUnique<Bounds>(inference.bounds());

            if (verbose)
            {
                std::cout << "Inferred: " << inference.bounds() << std::endl;
            }
        }
        else if (delta)
        {
            // If we were passed a bounds initially, it might not match the
            // inference we just performed.  Make sure our offset is consistent
            // with what we'll use as our bounds later.
            delta->offset() = boundsConforming->mid().apply([](double d)
            {
                const int64_t v(d);
                if (static_cast<double>(v / 10 * 10) == d) return v;
                else return (v + 10) / 10 * 10;
            });
        }

        if (!schema)
        {
            auto dims(inference.schema().dims());
            if (delta)
            {
                const Bounds cube(
                        Metadata::makeScaledCube(
                            *boundsConforming,
                            delta.get()));
                dims = Schema::deltify(cube, *delta, inference.schema()).dims();
            }

            const std::size_t pointIdSize([&fileInfo]()
            {
                std::size_t max(0);
                for (const auto& f : fileInfo)
                {
                    max = std::max(max, f.numPoints());
                }

                if (max <= std::numeric_limits<uint32_t>::max()) return 4;
                else return 8;
            }());

            const std::size_t originSize([&fileInfo]()
            {
                if (fileInfo.size() <= std::numeric_limits<uint16_t>::max())
                    return 2;
                if (fileInfo.size() <= std::numeric_limits<uint32_t>::max())
                    return 4;
                else
                    return 8;
            }());

            dims.emplace_back("OriginId", "unsigned", originSize);

            if (storePointId)
            {
                dims.emplace_back("PointId", "unsigned", pointIdSize);
            }

            schema = makeUnique<Schema>(dims);
        }

        if (!numPointsHint) numPointsHint = inference.numPoints();

        if (!transformation)
        {
            if (const std::vector<double>* t = inference.transformation())
            {
                transformation = makeUnique<std::vector<double>>(*t);
            }
        }
    }

    auto subset(maybeAccommodateSubset(json, *boundsConforming, delta.get()));
    json["numPointsHint"] = static_cast<Json::UInt64>(numPointsHint);

    const double density(densityLowerBound(fileInfo));
    if (!json.isMember("density")) json["density"] = density;

    Structure structure(json);
    const auto pre(structure.sparseDepthBegin());
    if (structure.applyDensity(density, boundsConforming->cubeify()))
    {
        const auto post(structure.sparseDepthBegin());
        if (post > pre)
        {
            std::cout << "Applied density " <<
                "(+" << (post - pre) << ")" << std::endl;
        }
    }

    Structure hierarchyStructure(Hierarchy::structure(structure, subset.get()));
    const HierarchyCompression hierarchyCompression(HierarchyCompression::Lzma);

    const auto ep(arbiter->getEndpoint(json["output"].asString()));
    const Manifest manifest(fileInfo, ep);

    const Metadata metadata(
            *boundsConforming,
            *schema,
            structure,
            hierarchyStructure,
            manifest,
            trustHeaders,
            storage,
            hierarchyCompression,
            density,
            reprojection.get(),
            subset.get(),
            delta.get(),
            transformation.get(),
            cesiumSettings.get());

    OuterScope outerScope;
    outerScope.setArbiter(arbiter);

    auto builder = makeUnique<Builder>(metadata, out, tmp, threads, outerScope);

    if (verbose) builder->verbose(true);
    return builder;
}

std::unique_ptr<Builder> ConfigParser::tryGetExisting(
        const Json::Value& config,
        std::shared_ptr<arbiter::Arbiter> arbiter,
        const std::string& outPath,
        const std::string& tmpPath,
        const std::size_t numThreads)
{
    std::unique_ptr<Builder> builder;
    std::unique_ptr<std::size_t> subsetId;

    if (config.isMember("subset"))
    {
        subsetId = makeUnique<std::size_t>(config["subset"]["id"].asUInt64());
    }

    OuterScope os;
    os.setArbiter(arbiter);

    return Builder::tryCreateExisting(
            outPath,
            tmpPath,
            numThreads,
            subsetId.get(),
            os);
}

void ConfigParser::normalizeInput(
        Json::Value& json,
        const arbiter::Arbiter& arbiter)
{
    Json::Value& input(json["input"]);
    const bool verbose(json["verbose"].asBool());

    const std::string extension(
            input.isString() ?
                arbiter::Arbiter::getExtension(input.asString()) : "");

    const bool isInferencePath(extension == "entwine-inference");

    if (!isInferencePath)
    {
        // The input source is a path or array of paths.  First, we possibly
        // need to expand out directories into their containing files.
        FileInfoList fileInfo;

        auto insert([&fileInfo, &arbiter, verbose](std::string in)
        {
            Paths current(arbiter.resolve(in, verbose));
            std::sort(current.begin(), current.end());
            for (const auto& c : current) fileInfo.emplace_back(c);
        });

        if (input.isArray())
        {
            for (const auto& entry : input)
            {
                if (entry.isString())
                {
                    insert(directorify(entry.asString()));
                }
                else
                {
                    fileInfo.emplace_back(entry);
                }
            }
        }
        else if (input.isString())
        {
            insert(directorify(input.asString()));
        }
        else return;

        // Now, we have an array of files (no directories).
        //
        // Reset our input with our resolved paths.  config.input.fileInfo will
        // be an array of strings, containing only paths with no associated
        // information.
        input = Json::Value();
        input.resize(fileInfo.size());
        for (std::size_t i(0); i < fileInfo.size(); ++i)
        {
            input[Json::ArrayIndex(i)] = fileInfo[i].toJson();
        }
    }
    else if (isInferencePath)
    {
        const std::string path(input.asString());
        const Json::Value inference(parse(arbiter.get(path)));

        input = inference["fileInfo"];

        if (!json.isMember("schema")) json["schema"] = inference["schema"];
        if (!json.isMember("bounds")) json["bounds"] = inference["bounds"];
        if (!json.isMember("numPointsHint"))
        {
            json["numPointsHint"] = inference["numPoints"];
        }

        if (inference.isMember("reprojection"))
        {
            json["reprojection"] = inference["reprojection"];
        }

        if (Delta::existsIn(inference))
        {
            if (!json.isMember("scale")) json["scale"] = inference["scale"];
            if (!json.isMember("offset")) json["offset"] = inference["offset"];
        }
    }
}

std::string ConfigParser::directorify(const std::string rawPath)
{
    std::string s(rawPath);

    if (s.size() && s.back() != '*')
    {
        if (arbiter::util::isDirectory(s))
        {
            s += '*';
        }
        else if (
                arbiter::util::getBasename(s).find_first_of('.') ==
                std::string::npos)
        {
            s += "/*";
        }
    }

    return s;
}

std::unique_ptr<Subset> ConfigParser::maybeAccommodateSubset(
        Json::Value& json,
        const Bounds& boundsConforming,
        const Delta* delta)
{
    std::unique_ptr<Subset> subset;
    const bool verbose(json["verbose"].asBool());

    if (json.isMember("subset"))
    {
        Bounds cube(Metadata::makeNativeCube(boundsConforming, delta));
        subset = makeUnique<Subset>(cube, json["subset"]);
        const std::size_t configNullDepth(json["nullDepth"].asUInt64());
        const std::size_t minimumNullDepth(subset->minimumNullDepth());

        if (configNullDepth < minimumNullDepth)
        {
            if (verbose)
            {
                std::cout <<
                    "Bumping null depth to accomodate subset: " <<
                    minimumNullDepth << std::endl;
            }

            json["nullDepth"] = Json::UInt64(minimumNullDepth);
        }

        const std::size_t configBaseDepth(json["baseDepth"].asUInt64());
        const std::size_t ppc(json["pointsPerChunk"].asUInt64());
        const std::size_t minimumBaseDepth(subset->minimumBaseDepth(ppc));

        if (configBaseDepth < minimumBaseDepth)
        {
            if (verbose)
            {
                std::cout <<
                    "Bumping base depth to accomodate subset: " <<
                    minimumBaseDepth << std::endl;
            }

            json["baseDepth"] = Json::UInt64(minimumBaseDepth);
            json["bumpDepth"] = Json::UInt64(configBaseDepth);
        }
    }

    return subset;
}

} // namespace entwine


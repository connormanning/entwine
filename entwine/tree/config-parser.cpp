/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <limits>
#include <numeric>

#include <entwine/tree/config-parser.hpp>

#include <entwine/formats/cesium/settings.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/manifest.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/format.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/inference.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    Json::Reader reader;

    std::unique_ptr<Bounds> getBounds(const Json::Value& json)
    {
        std::unique_ptr<Bounds> bounds;
        if (!json.empty()) bounds = makeUnique<Bounds>(json);
        return bounds;
    }

    std::unique_ptr<Reprojection> getReprojection(const Json::Value& json)
    {
        std::unique_ptr<Reprojection> reprojection;
        if (!json.empty()) reprojection = makeUnique<Reprojection>(json);
        return reprojection;
    }

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
    json["tmp"] = "tmp";
    json["threads"] = 8;
    json["trustHeaders"] = true;
    json["prefixIds"] = false;
    json["pointsPerChunk"] = 262144;
    json["numPointsHint"] = Json::Value::null;
    json["bounds"] = Json::Value::null;
    json["schema"] = Json::Value::null;
    json["compress"] = true;
    json["nullDepth"] = 7;
    json["baseDepth"] = 10;

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

    extractManifest(json, *arbiter);

    const std::string outPath(json["output"].asString());
    const std::string tmpPath(json["tmp"].asString());
    const bool compress(json["compress"].asUInt64());
    const bool force(json["force"].asBool());
    const bool trustHeaders(json["trustHeaders"].asBool());
    const std::size_t threads(json["threads"].asUInt64());

    auto manifest(makeUnique<Manifest>(json["input"]));
    auto cesiumSettings(getCesiumSettings(json["formats"]));
    bool absolute(json["absolute"].asBool());

    if (cesiumSettings)
    {
        absolute = true;
        json["reprojection"]["out"] = "EPSG:4978";
    }

    auto reprojection(getReprojection(json["reprojection"]));

    if (!force)
    {
        auto builder = tryGetExisting(
                json,
                *arbiter,
                outPath,
                tmpPath,
                threads);

        if (builder)
        {
            if (manifest) builder->append(*manifest);
            return builder;
        }
    }

    std::unique_ptr<std::vector<double>> transformation;
    std::unique_ptr<Delta> delta;
    if (!absolute && Delta::existsIn(json)) delta = makeUnique<Delta>(json);

    std::size_t numPointsHint(json["numPointsHint"].asUInt64());

    if (!numPointsHint && manifest)
    {
        numPointsHint = std::accumulate(
                manifest->paths().begin(),
                manifest->paths().end(),
                std::size_t(0),
                [](std::size_t sum, const FileInfo& f)
                {
                    return sum + f.numPoints();
                });
    }

    auto boundsConforming(getBounds(json["bounds"]));
    auto schema(
            json["schema"].isNull() ?
                std::unique_ptr<Schema>() : makeUnique<Schema>(json["schema"]));

    const bool needsInference(!boundsConforming || !schema || !numPointsHint);

    if (manifest && needsInference)
    {
        if (verbose)
        {
            std::cout << "Performing dataset inference..." << std::endl;
        }

        Inference inference(
                *manifest,
                reprojection.get(),
                trustHeaders,
                !absolute,
                tmpPath,
                threads,
                verbose,
                arbiter.get(),
                !!cesiumSettings);

        inference.go();
        manifest = makeUnique<Manifest>(inference.manifest());

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
            boundsConforming.reset(new Bounds(inference.nativeBounds()));

            if (verbose)
            {
                std::cout << "Inferred: " << inference.nativeBounds() <<
                    std::endl;
            }
        }

        if (!schema)
        {
            auto dims(inference.schema().dims());
            if (delta)
            {
                Bounds cube(boundsConforming->cubeify(*delta));
                dims = Schema::deltify(cube, *delta, inference.schema()).dims();
            }

            const std::size_t pointIdSize([&manifest]()
            {
                std::size_t max(0);
                for (std::size_t i(0); i < manifest->size(); ++i)
                {
                    max = std::max(max, manifest->get(i).numPoints());
                }

                if (max <= std::numeric_limits<uint32_t>::max()) return 4;
                else return 8;
            }());

            const std::size_t originSize([&manifest]()
            {
                if (manifest->size() <= std::numeric_limits<uint32_t>::max())
                    return 4;
                else
                    return 8;
            }());

            dims.emplace_back("PointId", "unsigned", pointIdSize);
            dims.emplace_back("OriginId", "unsigned", originSize);

            schema = makeUnique<Schema>(dims);
        }

        if (!numPointsHint) numPointsHint = inference.numPoints();

        if (const std::vector<double>* t = inference.transformation())
        {
            transformation = makeUnique<std::vector<double>>(*t);
        }
    }

    auto subset(maybeAccommodateSubset(json, *boundsConforming, delta.get()));


    json["numPointsHint"] = static_cast<Json::UInt64>(numPointsHint);
    Structure structure(json);
    Structure hierarchyStructure(Hierarchy::structure(structure, subset.get()));
    const HierarchyCompression hierarchyCompression(
            compress ? HierarchyCompression::Lzma : HierarchyCompression::None);

    const Metadata metadata(
            *boundsConforming,
            *schema,
            structure,
            hierarchyStructure,
            *manifest,
            trustHeaders,
            compress,
            hierarchyCompression,
            reprojection.get(),
            subset.get(),
            delta.get(),
            transformation.get(),
            cesiumSettings.get());

    OuterScope outerScope;
    outerScope.setArbiter(arbiter);

    return makeUnique<Builder>(metadata, outPath, tmpPath, threads, outerScope);
}

std::unique_ptr<Builder> ConfigParser::tryGetExisting(
        const Json::Value& config,
        const arbiter::Arbiter& arbiter,
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

    const std::string postfix(subsetId ? "-" + std::to_string(*subsetId) : "");

    if (arbiter.getEndpoint(outPath).tryGetSize("entwine" + postfix))
    {
        builder = makeUnique<Builder>(outPath, tmpPath, numThreads);
    }

    return builder;
}

void ConfigParser::extractManifest(
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
        // The input source is a path or array of paths.
        std::vector<std::string> paths;

        auto insert([&paths, &arbiter, verbose](std::string in)
        {
            std::vector<std::string> current(arbiter.resolve(in, verbose));
            paths.insert(paths.end(), current.begin(), current.end());
        });

        if (input.isArray())
        {
            for (const auto& path : input) insert(directorify(path.asString()));
        }
        else
        {
            insert(directorify(input.asString()));
        }

        // Reset our input with our resolved paths.
        input = Json::Value();
        auto& fileInfo(input["fileInfo"]);
        fileInfo.resize(paths.size());
        for (std::size_t i(0); i < paths.size(); ++i)
        {
            fileInfo[Json::ArrayIndex(i)]["path"] = paths[i];
        }
    }
    else if (isInferencePath)
    {
        const std::string path(input.asString());
        const Json::Value inference(parse(arbiter.get(path)));

        input = inference["manifest"];
        json["schema"] = inference["schema"];
        json["bounds"] = inference["bounds"];
        json["numPointsHint"] = inference["numPoints"];

        if (inference.isMember("reprojection"))
        {
            json["reprojection"] = inference["reprojection"];
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
        Bounds cube(boundsConforming.cubeify(delta));
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


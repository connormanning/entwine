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

Json::Value ConfigParser::unflatten(Json::Value in)
{
    Json::Value out;

    for (const auto& key : in.getMemberNames())
    {
        if (in[key].isObject()) out[key] = in[key];
    }

    auto maybeUnflatten([&in, &out](std::string nest, std::string key)
    {
        if (in.isMember(key))
        {
            if (out[nest].isMember(key))
            {
                throw std::runtime_error("Duplicate specification of " + key);
            }

            out[nest][key] = in[key];
        }
    });

    // These few will be written to different keys then their unflattened
    // versions.
    if (in.isMember("input") && !in["input"].isObject())
    {
        out["input"]["manifest"] = in["input"];
    }

    if (in.isMember("output") && !in["output"].isObject())
    {
        out["output"]["path"] = in["output"];
    }

    if (in.isMember("reprojection"))
    {
        out["geometry"]["reproject"] = in["reprojection"];
    }

    maybeUnflatten("input", "threads");
    maybeUnflatten("input", "trustHeaders");
    maybeUnflatten("input", "run");

    maybeUnflatten("output", "tmp");
    maybeUnflatten("output", "compress");
    maybeUnflatten("output", "force");

    maybeUnflatten("structure", "numPointsHint");
    maybeUnflatten("structure", "nullDepth");
    maybeUnflatten("structure", "baseDepth");
    maybeUnflatten("structure", "coldDepth");
    maybeUnflatten("structure", "dynamicChunks");
    maybeUnflatten("structure", "pointsPerChunk");
    maybeUnflatten("structure", "type");
    maybeUnflatten("structure", "prefixIds");

    maybeUnflatten("geometry", "bounds");
    maybeUnflatten("geometry", "schema");
    maybeUnflatten("geometry", "reproject");

    return out;
}

std::unique_ptr<Builder> ConfigParser::getBuilder(
        Json::Value config,
        std::shared_ptr<arbiter::Arbiter> arbiter)
{
    extractManifest(config, *arbiter);

    const Json::Value& jsonInput(config["input"]);
    const Json::Value& jsonOutput(config["output"]);
    const Json::Value& jsonGeometry(config["geometry"]);
    Json::Value& jsonStructure(config["structure"]);

    // Build specifications and path info.
    const std::string outPath(jsonOutput["path"].asString());
    const std::string tmpPath(jsonOutput["tmp"].asString());
    const bool compress(jsonOutput["compress"].asUInt64());
    const bool force(jsonOutput["force"].asBool());
    const bool absolute(config["absolute"].asBool());

    // Indexing parameters.
    const bool trustHeaders(jsonInput["trustHeaders"].asBool());
    const std::size_t threads(jsonInput["threads"].asUInt64());

    // Geometry and spatial info.
    auto boundsConforming(getBounds(jsonGeometry["bounds"]));
    auto schema(
            jsonGeometry["schema"].isNull() ?
                std::unique_ptr<Schema>() :
                makeUnique<Schema>(jsonGeometry["schema"]));

    std::size_t numPointsHint(jsonStructure["numPointsHint"].asUInt64());

    auto manifest(makeUnique<Manifest>(config["input"]["manifest"]));
    auto cesiumSettings(getCesiumSettings(config["formats"]));

    Json::Value r(jsonGeometry["reproject"]);
    if (cesiumSettings) r["out"] = "EPSG:4978";
    auto reprojection(getReprojection(r));

    if (!force)
    {
        auto builder = tryGetExisting(
                config,
                *arbiter,
                outPath,
                tmpPath,
                threads);

        if (builder)
        {
            builder->append(*manifest);
            return builder;
        }
    }

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

    const bool needsInference(!boundsConforming || !schema || !numPointsHint);

    std::unique_ptr<std::vector<double>> transformation;
    std::unique_ptr<Delta> delta;

    if (Delta::existsIn(config)) delta = makeUnique<Delta>(config);

    if (manifest && needsInference)
    {
        std::cout << "Performing dataset inference..." << std::endl;
        Inference inference(
                *manifest,
                tmpPath,
                threads,
                true,
                reprojection.get(),
                trustHeaders,
                !absolute,
                arbiter.get(),
                !!cesiumSettings);

        inference.go();
        manifest.reset(new Manifest(inference.manifest()));

        if (!absolute && inference.delta())
        {
            if (!delta) delta = makeUnique<Delta>();

            if (!config.isMember("scale"))
            {
                delta->scale() = inference.delta()->scale();
            }

            if (!config.isMember("offset"))
            {
                delta->offset() = inference.delta()->offset();
            }
        }

        if (!boundsConforming)
        {
            boundsConforming.reset(new Bounds(inference.nativeBounds()));
            std::cout << "Inferred: " << inference.nativeBounds() << std::endl;
        }

        if (!schema)
        {
            auto dims(inference.schema().dims());
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

    std::unique_ptr<Subset> subset;

    if (config.isMember("subset"))
    {
        Bounds cube(boundsConforming->cubeify(delta.get()));
        subset = makeUnique<Subset>(cube, config["subset"]);

        const std::size_t configNullDepth(
                jsonStructure["nullDepth"].asUInt64());

        const std::size_t minimumNullDepth(subset->minimumNullDepth());

        if (configNullDepth < minimumNullDepth)
        {
            std::cout <<
                "Bumping null depth to accomodate subset: " <<
                minimumNullDepth << std::endl;

            jsonStructure["nullDepth"] = Json::UInt64(minimumNullDepth);
        }

        const std::size_t configBaseDepth(
                jsonStructure["baseDepth"].asUInt64());

        const std::size_t ppc(jsonStructure["pointsPerChunk"].asUInt64());

        const std::size_t minimumBaseDepth(subset->minimumBaseDepth(ppc));

        if (configBaseDepth < minimumBaseDepth)
        {
            std::cout <<
                "Bumping base depth to accomodate subset: " <<
                minimumBaseDepth << std::endl;

            jsonStructure["baseDepth"] = Json::UInt64(minimumBaseDepth);
            jsonStructure["bumpDepth"] = Json::UInt64(configBaseDepth);
        }
    }

    jsonStructure["numPointsHint"] = static_cast<Json::UInt64>(numPointsHint);
    Structure structure(jsonStructure);
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
    std::unique_ptr<std::size_t> splitId;

    if (config.isMember("subset"))
    {
        subsetId = makeUnique<std::size_t>(config["subset"]["id"].asUInt64());
    }

    const Json::Value& input(config["input"]);

    if (
            input.isMember("manifest") &&
            input["manifest"].isObject() &&
            input["manifest"].isMember("split"))
    {
        splitId = makeUnique<std::size_t>(
                input["manifest"]["split"]["id"].asUInt64());
    }

    const std::string postfix(
            (subsetId ? "-" + std::to_string(*subsetId) : "") +
            (splitId ? "-" + std::to_string(*splitId) : ""));

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
    Json::Value& jsonManifest(input["manifest"]);

    const bool isInferencePath(
            jsonManifest.isString() &&
            arbiter::Arbiter::getExtension(jsonManifest.asString()) == "eninf");

    bool extractingPaths(
            (jsonManifest.isString() && !isInferencePath) ||
            jsonManifest.isArray());

    if (extractingPaths)
    {
        // The input source is a path or array of paths.
        std::vector<std::string> paths;

        auto insert([&paths, &arbiter](std::string in)
        {
            std::vector<std::string> current(arbiter.resolve(in, true));
            paths.insert(paths.end(), current.begin(), current.end());
        });

        if (jsonManifest.isArray())
        {
            for (Json::ArrayIndex i(0); i < jsonManifest.size(); ++i)
            {
                insert(jsonManifest[i].asString());
            }
        }
        else
        {
            insert(directorify(jsonManifest.asString()));
        }

        jsonManifest = Json::Value();
        auto& fileInfo(jsonManifest["fileInfo"]);
        fileInfo.resize(paths.size());
        for (std::size_t i(0); i < paths.size(); ++i)
        {
            fileInfo[Json::ArrayIndex(i)]["path"] = paths[i];
        }
    }
    else
    {
        if (isInferencePath)
        {
            const std::string path(jsonManifest.asString());
            const Json::Value inference(parse(arbiter.get(path)));

            jsonManifest = inference["manifest"];
            json["geometry"]["schema"] = inference["schema"];
            json["geometry"]["bounds"] = inference["bounds"];
            json["structure"]["numPointsHint"] = inference["numPoints"];
            if (inference.isMember("reproject"))
            {
                json["reproject"] = inference["reproject"];
            }
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

} // namespace entwine


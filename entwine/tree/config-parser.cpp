/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/config-parser.hpp>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>

namespace entwine
{

namespace
{
    Json::Reader reader;

    std::size_t getDimensions(const Json::Value& jsonType)
    {
        const std::string typeString(jsonType.asString());

        if (typeString == "hybrid") return 2;
        else if (typeString == "quadtree") return 2;
        else if (typeString == "octree") return 3;
        else throw std::runtime_error("Invalid tree type");
    }

    std::unique_ptr<BBox> getBBox(const Json::Value& json, const bool is3d)
    {
        std::unique_ptr<BBox> bbox;

        if (!json.empty())
        {
            if (json.size() == 4 && !is3d)
            {
                Json::Value expanded;
                Json::Value& bounds(expanded["bounds"]);

                bounds.append(json[0].asDouble());
                bounds.append(json[1].asDouble());
                bounds.append(std::numeric_limits<double>::max());
                bounds.append(json[2].asDouble());
                bounds.append(json[3].asDouble());
                bounds.append(std::numeric_limits<double>::lowest());

                expanded["is3d"] = false;

                bbox.reset(new BBox(expanded));
            }
            else if (is3d)
            {
                Json::Value expanded;
                expanded["bounds"] = json;
                expanded["is3d"] = true;
                bbox.reset(new BBox(expanded));
            }
            else
            {
                throw std::runtime_error(
                        "Invalid bbox for the requested tree type.");
            }
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
}

std::unique_ptr<Builder> ConfigParser::getBuilder(
        const Json::Value& config,
        std::shared_ptr<arbiter::Arbiter> arbiter,
        const RunInfo& runInfo)
{
    std::unique_ptr<Builder> builder;

    // Indexing parameters.
    const Json::Value jsonInput(config["input"]);
    const bool trustHeaders(jsonInput["trustHeaders"].asBool());
    const std::size_t threads(jsonInput["threads"].asUInt64());

    // Build specifications and path info.
    const Json::Value& jsonOutput(config["output"]);
    const std::string outPath(jsonOutput["path"].asString());
    const std::string tmpPath(jsonOutput["tmp"].asString());
    const bool outCompress(jsonOutput["compress"].asUInt64());
    const bool force(jsonOutput["force"].asBool());

    // Tree structure.
    const Json::Value& jsonStructure(config["structure"]);
    const std::size_t nullDepth(jsonStructure["nullDepth"].asUInt64());
    const std::size_t baseDepth(jsonStructure["baseDepth"].asUInt64());
    const std::size_t chunkPoints(jsonStructure["pointsPerChunk"].asUInt64());
    const std::size_t dimensions(getDimensions(jsonStructure["type"]));
    const bool tubular(jsonStructure["type"].asString() == "hybrid");
    const bool lossless(!jsonStructure.isMember("coldDepth"));
    const bool dynamicChunks(jsonStructure["dynamicChunks"].asBool());

    std::pair<std::size_t, std::size_t> subset({ 0, 0 });
    if (jsonStructure.isMember("subset"))
    {
        subset = std::make_pair(
                jsonStructure["subset"]["id"].asUInt64(),
                jsonStructure["subset"]["of"].asUInt64());
    }

    const std::size_t numPointsHint(
            jsonStructure.isMember("numPointsHint") ?
                jsonStructure["numPointsHint"].asUInt64() : 0);

    // Geometry and spatial info.
    const Json::Value& geometry(config["geometry"]);
    auto bbox(getBBox(geometry["bbox"], dimensions == 3 || tubular));
    auto reprojection(getReprojection(geometry["reproject"]));
    Schema schema(geometry["schema"]);

    const Structure structure(([&]()
    {
        if (lossless)
        {
            return Structure(
                    nullDepth,
                    baseDepth,
                    chunkPoints,
                    dimensions,
                    numPointsHint,
                    tubular,
                    dynamicChunks,
                    bbox.get(),
                    subset);
        }
        else
        {
            return Structure(
                    nullDepth,
                    baseDepth,
                    jsonStructure["coldDepth"].asUInt64(),
                    chunkPoints,
                    dimensions,
                    numPointsHint,
                    tubular,
                    dynamicChunks,
                    bbox.get(),
                    subset);
        }
    })());

    bool exists(false);

    if (!force)
    {
        try
        {
            // TODO Existence test won't work for partially-complete subsets.
            // Add subset extension to outPath.
            arbiter::Endpoint endpoint(arbiter->getEndpoint(outPath));
            if (endpoint.getSubpath("entwine").size())
            {
                exists = true;
            }
        }
        catch (...)
        {
            // Nothing to do here.
        }
    }

    if (!force && exists)
    {
        builder.reset(new Builder(outPath, tmpPath, threads, arbiter));
    }
    else
    {
        if (!bbox && runInfo.manifest.size() > 1)
        {
            throw std::runtime_error(
                    "Can't infer bounds from multiple sources");
        }

        builder.reset(
                new Builder(
                    outPath,
                    tmpPath,
                    outCompress,
                    trustHeaders,
                    reprojection.get(),
                    bbox.get(),
                    schema.dims(),
                    threads,
                    structure,
                    arbiter));
    }

    return builder;
}

RunInfo ConfigParser::getRunInfo(
        const Json::Value& json,
        const arbiter::Arbiter& arbiter)
{
    const Json::Value& input(json["input"]);
    const Json::Value& jsonManifest(input["manifest"]);

    std::vector<std::string> manifest;

    auto insert([&manifest, &arbiter](std::string in)
    {
        std::vector<std::string> paths(arbiter.resolve(in, true));
        manifest.insert(manifest.end(), paths.begin(), paths.end());
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
        insert(jsonManifest.asString());
    }

    const std::size_t jsonRunCount(
            input.isMember("run") && input["run"].asUInt64() ?
                input["run"].asUInt64() :
                manifest.size());

    const std::size_t runCount(
            std::min<std::size_t>(jsonRunCount, manifest.size()));

    return RunInfo(manifest, runCount);
}

Json::Value ConfigParser::parse(const std::string& input)
{
    Json::Value json;

    if (input.size())
    {
        reader.parse(input, json, false);

        const std::string jsonError(reader.getFormattedErrorMessages());
        if (!jsonError.empty())
        {
            throw std::runtime_error("Error during parsing: " + jsonError);
        }
    }

    return json;
}

} // namespace entwine


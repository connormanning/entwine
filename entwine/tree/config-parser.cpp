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

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/manifest.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/inference.hpp>

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
        if (!json.empty()) bbox.reset(new BBox(json));
        return bbox;
    }

    std::unique_ptr<Reprojection> getReprojection(const Json::Value& json)
    {
        std::unique_ptr<Reprojection> reprojection;

        if (!json.empty())
        {
            reprojection.reset(new Reprojection(json));
        }

        return reprojection;
    }

    std::unique_ptr<Subset> getSubset(
            const Json::Value& json,
            Structure& structure,
            const BBox& bbox)
    {
        std::unique_ptr<Subset> subset;

        if (json.isMember("subset"))
        {
            subset.reset(new Subset(structure, bbox, json["subset"]));
        }

        return subset;
    }
}

std::unique_ptr<Builder> ConfigParser::getBuilder(
        const Json::Value& config,
        std::shared_ptr<arbiter::Arbiter> arbiter,
        std::unique_ptr<Manifest> manifest)
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
    const std::size_t coldDepth(
            (jsonStructure.isMember("coldDepth") &&
            jsonStructure["coldDepth"].isIntegral()) ?
                jsonStructure["coldDepth"].asUInt64() : 0);

    const std::size_t chunkPoints(jsonStructure["pointsPerChunk"].asUInt64());
    const std::size_t dimensions(getDimensions(jsonStructure["type"]));
    const bool tubular(jsonStructure["type"].asString() == "hybrid");
    const bool dynamicChunks(jsonStructure["dynamicChunks"].asBool());
    const bool discardDuplicates(jsonStructure["discardDuplicates"].asBool());
    const bool prefixIds(jsonStructure["prefixIds"].asBool());

    std::size_t numPointsHint(
            jsonStructure.isMember("numPointsHint") ?
                jsonStructure["numPointsHint"].asUInt64() : 0);

    // Geometry and spatial info.
    const Json::Value& geometry(config["geometry"]);
    auto bboxConforming(getBBox(geometry["bbox"], dimensions == 3 || tubular));
    auto reprojection(getReprojection(geometry["reproject"]));
    Schema schema(geometry["schema"]);

    bool exists(false);
    std::string postfix;

    if (!force)
    {
        // TODO Should probably just try to construct a Builder here using
        // the subset/split constructor instead of reimplementing the postfix
        // logic.
        if (config.isMember("subset"))
        {
            const std::size_t id(config["subset"]["id"].asUInt64() - 1);
            postfix += "-" + std::to_string(id);
        }

        if (manifest && manifest->split() && manifest->split()->begin())
        {
            postfix += "-" + std::to_string(manifest->split()->begin());
        }

        arbiter::Endpoint endpoint(arbiter->getEndpoint(outPath));
        if (endpoint.tryGetSubpath("entwine" + postfix))
        {
            exists = true;
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

    if (
            manifest && !exists &&
            (!bboxConforming || !schema.pointSize() || !numPointsHint))
    {
        std::cout << "Performing dataset inference..." << std::endl;
        Inference inference(
                *manifest,
                tmpPath,
                threads,
                true,
                reprojection.get(),
                trustHeaders,
                arbiter.get());

        inference.go();
        manifest.reset(new Manifest(inference.manifest()));

        if (!bboxConforming)
        {
            bboxConforming.reset(new BBox(inference.bbox()));
            std::cout << "Inferred: " << inference.bbox() << std::endl;
        }

        if (!schema.pointSize())
        {
            auto dims(inference.schema().dims());
            const std::size_t originSize([&manifest]()
            {
                if (manifest->size() <= std::numeric_limits<uint32_t>::max())
                    return 4;
                else
                    return 8;
            }());

            dims.emplace_back("Origin", "unsigned", originSize);

            schema = Schema(dims);
        }

        if (!numPointsHint) numPointsHint = inference.numPoints();
    }

    OuterScope outerScope;
    outerScope.setArbiter(arbiter);

    if (!force && exists)
    {
        builder.reset(
                new Builder(
                    outPath,
                    tmpPath,
                    threads,
                    postfix,
                    config["subset"],
                    outerScope));
    }
    else
    {
        if (!bboxConforming) throw std::runtime_error("Missing inference");

        Structure structure(
                nullDepth,
                baseDepth,
                coldDepth,
                chunkPoints,
                dimensions,
                numPointsHint,
                tubular,
                dynamicChunks,
                discardDuplicates,
                prefixIds);

        // TODO This cubeifying code is duplicated from the Builder constructor.
        BBox cube(*bboxConforming);
        if (!cube.isCubic())
        {
            cube.growBy(0.005);
            cube.cubeify();
        }

        std::unique_ptr<Subset> subset(getSubset(config, structure, cube));

        builder.reset(
                new Builder(
                    std::move(manifest),
                    outPath,
                    tmpPath,
                    outCompress,
                    trustHeaders,
                    subset.get(),
                    reprojection.get(),
                    *bboxConforming,
                    schema,
                    threads,
                    structure,
                    outerScope));
    }

    return builder;
}

std::unique_ptr<Manifest> ConfigParser::getManifest(
        const Json::Value& json,
        const arbiter::Arbiter& arbiter)
{
    std::unique_ptr<Manifest> manifest;

    const Json::Value& input(json["input"]);
    const Json::Value& jsonManifest(input["manifest"]);

    if (jsonManifest.isString() || jsonManifest.isArray())
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
            insert(jsonManifest.asString());
        }

        manifest.reset(new Manifest(paths));
    }
    else if (jsonManifest.isObject())
    {
        // The input source is a previously inferred manifest.
        manifest.reset(new Manifest(jsonManifest));
    }

    return manifest;
}

Json::Value ConfigParser::parse(const std::string& input)
{
    Json::Value json;

    if (input.size())
    {
        if (!reader.parse(input, json, false))
        {
            const std::string jsonError(reader.getFormattedErrorMessages());
            if (!jsonError.empty())
            {
                throw std::runtime_error("Error during parsing: " + jsonError);
            }
        }
    }

    return json;
}

} // namespace entwine


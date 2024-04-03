/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/util/config.hpp>

#include <cassert>
#include <cmath>
#include <memory>

#include <entwine/builder/heuristics.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/exceptions.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/pipeline.hpp>

namespace entwine
{
namespace config
{

namespace
{

BuildParameters getBuildParameters(const json& j)
{
    return BuildParameters(
        getMinNodeSize(j),
        getMaxNodeSize(j),
        getCacheSize(j),
        getSleepCount(j),
        getProgressInterval(j),
        getHierarchyStep(j),
        getVerbose(j),
        j.value("laz_14", false));
}

} // unnamed namespace

Endpoints getEndpoints(const json& j)
{
    const auto arbiter = std::shared_ptr<arbiter::Arbiter>(
        getArbiter(j).release());
    const auto output = getOutput(j);
    const auto tmp = getTmp(j);

    if (!output.size()) throw ConfigurationError("Missing 'output'");
    if (!tmp.size()) throw ConfigurationError("Missing 'tmp'");

    return Endpoints(arbiter, output, tmp);
}

Metadata getMetadata(const json& j)
{
    return Metadata(
        getEptVersion(j),
        getSchema(j),
        getBoundsConforming(j),
        getBounds(j),
        getSrs(j),
        getSubset(j),
        getDataType(j),
        getSpan(j),
        getBuildParameters(j));
}

std::unique_ptr<arbiter::Arbiter> getArbiter(const json& j)
{
    return std::unique_ptr<arbiter::Arbiter>(
        new arbiter::Arbiter(j.value("arbiter", json()).dump()));
}
StringList getInput(const json& j)
{
    if (!j.count("input")) return { };

    const json& input(j.at("input"));
    if (input.is_string()) return StringList(1, input.get<std::string>());
    else return input.get<StringList>();
}
std::string getOutput(const json& j) { return j.value("output", ""); }
std::string getTmp(const json& j)
{
    return j.value("tmp", arbiter::getTempPath());
}

io::Type getDataType(const json& j)
{
    return j.value("dataType", io::Type::Laszip);
}

// Bounds may be specified in one of two formats, depending on the context:
// 1: Only "bounds" exists, in which case it represents the conforming bounds.
// 2: Both "bounds" and "boundsConforming" exist.
Bounds getBoundsConforming(const json& j)
{
    if (j.count("boundsConforming"))
    {
        return j.at("boundsConforming").get<Bounds>();
    }

    // Bloat the conforming bounds to the nearest integer.
    const Bounds b = j.at("bounds").get<Bounds>();
    Point min = b.min();
    Point max = b.max();
    for (unsigned i = 0; i < 3; ++i)
    {
        if (isIntegral(min[i])) --min[i];
        else min[i] = std::floor(min[i]);
    }
    for (unsigned i = 0; i < 3; ++i)
    {
        if (isIntegral(max[i])) ++max[i];
        else max[i] = std::ceil(max[i]);
    }
    return Bounds(min, max);
}
Bounds getBounds(const json& j)
{
    const Bounds b = j.at("bounds").get<Bounds>();
    return j.count("boundsConforming") ? b : cubeify(b);
}
Schema getSchema(const json& j)
{
    Schema schema = j.at("schema").get<Schema>();
    if (getAbsolute(j))
    {
        schema = setScaleOffset(schema, ScaleOffset(1, 0));
    }
    else if (const auto scale = getScale(j))
    {
        schema = setScaleOffset(schema, *scale);
    }
    else if (!getScaleOffset(schema))
    {
        schema = setScaleOffset(schema, Scale(0.01));
    }

    // If we have a scale set, calculate an offset.
    if (auto so = getScaleOffset(schema))
    {
        if (so->scale != 1)
        {
            so->offset = getBounds(j).mid().round();
            schema = setScaleOffset(schema, *so);
        }
    }

    if (getAllowOriginId(j) && !contains(schema, "OriginId"))
    {
        schema.emplace_back("OriginId", Type::Unsigned32);
    }

    return schema;
}
optional<Reprojection> getReprojection(const json& j)
{
    // TODO: For some reason this is not working for Reprojection although we
    // use this pattern for several other classes.  Should look into it but the
    // below works properly.
    // return j.value("reprojection", optional<Reprojection>());

    if (j.count("reprojection")) return Reprojection(j.at("reprojection"));
    return optional<Reprojection>();
}
optional<Srs> getSrs(const json& j)
{
    if (j.count("srs"))
    {
        const auto srs = j.at("srs").get<Srs>();
        if (srs.exists()) return srs;
    }
    if (const auto reprojection = getReprojection(j))
    {
        return Srs(reprojection->out());
    }
    return { };
}
optional<Subset> getSubset(const json& j)
{
    return j.value("subset", optional<Subset>());
}
optional<Scale> getScale(const json& j)
{
    return j.value("scale", optional<Scale>());
}

json getPipeline(const json& j)
{
    json pipeline = j.value("pipeline", json::array({ json::object() }));
    if (pipeline.is_object()) pipeline = pipeline.at("pipeline");

    if (!pipeline.is_array() || pipeline.empty())
    {
        throw std::runtime_error("Invalid pipeline: " + pipeline.dump(2));
    }

    json& reader(pipeline.at(0));

    if (const auto reprojection = getReprojection(j))
    {
        // First set the input SRS on the reader if necessary.
        const std::string in(reprojection->in());
        if (in.size())
        {
            if (reprojection->hammer()) reader["override_srs"] = in;
            else reader["default_srs"] = in;
        }

        // Now set up the output.  If there's already a filters.reprojection in
        // the pipeline, we'll fill it in.  Otherwise, we'll append one.
        json& filter(findOrAppendStage(pipeline, "filters.reprojection"));
        filter.update({ {"out_srs", reprojection->out() } });
    }

    return pipeline;
}

unsigned getThreads(const json& j) { return j.value("threads", 8); }
Threads getCompoundThreads(const json& j)
{
    return Threads(j.value("threads", json()));
}
Version getEptVersion(const json& j)
{
    const std::string existing = j.value("version", "");
    if (existing.size() && existing != currentEptVersion().toString())
    {
        throw ConfigurationError("Cannot update a previous EPT version");
    }
    return currentEptVersion();
}

bool getVerbose(const json& j) { return j.value("verbose", true); }
bool getDeep(const json& j) { return j.value("deep", false); }
bool getStats(const json& j) { return j.value("stats", true); }
bool getForce(const json& j) { return j.value("force", false); }
bool getAbsolute(const json& j) { return j.value("absolute", false); }
bool getAllowOriginId(const json& j) { return j.value("allowOriginId", true); }

uint64_t getSpan(const json& j)
{
    return j.value("span", 128);
}
uint64_t getMinNodeSize(const json& j)
{
    const auto span = getSpan(j);
    return j.value("minNodeSize", span * span);
}
uint64_t getMaxNodeSize(const json& j)
{
    const auto span = getSpan(j);
    return j.value("maxNodeSize", span * span * 4);
}
uint64_t getCacheSize(const json& j)
{
    return j.value("cacheSize", heuristics::cacheSize);
}
uint64_t getSleepCount(const json& j)
{
    return j.value("sleepCount", heuristics::sleepCount);
}
uint64_t getProgressInterval(const json& j)
{
    return j.value("progressInterval", 10);
}
uint64_t getLimit(const json& j)
{
    return j.value("limit", 0);
}
uint64_t getHierarchyStep(const json& j)
{
    return j.value("hierarchyStep", 0);
}

} // namespace config
} // namespace entwine

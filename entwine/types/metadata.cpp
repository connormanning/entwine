/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/formats/cesium/settings.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/format.hpp>
#include <entwine/types/manifest.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/storage.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    const double epsilon(0.005);

    std::vector<std::string> fromJsonArray(const Json::Value& json)
    {
        std::vector<std::string> v;

        if (json.isNull() || !json.isArray()) return v;

        for (Json::ArrayIndex i(0); i < json.size(); ++i)
        {
            v.push_back(json[i].asString());
        }

        return v;
    }

    std::string getPostfix(const std::size_t* subsetId)
    {
        if (subsetId) return "-" + std::to_string(*subsetId);
        else return "";
    }
}

Metadata::Metadata(
        const Bounds& boundsNative,
        const Schema& schema,
        const Structure& structure,
        const Structure& hierarchyStructure,
        const Manifest& manifest,
        const bool trustHeaders,
        const bool compress,
        const HierarchyCompression hierarchyCompress,
        const Reprojection* reprojection,
        const Subset* subset,
        const Delta* delta,
        const Transformation* transformation,
        const cesium::Settings* cesiumSettings)
    : m_boundsNative(makeUnique<Bounds>(boundsNative))
    , m_boundsConforming(makeUnique<Bounds>(m_boundsNative->deltify(delta)))
    , m_boundsEpsilon(makeUnique<Bounds>(m_boundsConforming->growBy(epsilon)))
    , m_bounds(makeUnique<Bounds>(m_boundsNative->cubeify(delta)))
    , m_schema(makeUnique<Schema>(schema))
    , m_structure(makeUnique<Structure>(structure))
    , m_hierarchyStructure(makeUnique<Structure>(hierarchyStructure))
    , m_manifest(makeUnique<Manifest>(manifest))
    , m_delta(maybeClone(delta))
    , m_format(
            makeUnique<Format>(
                *this,
                trustHeaders,
                compress,
                hierarchyCompress))
    , m_reprojection(maybeClone(reprojection))
    , m_subset(maybeClone(subset))
    , m_transformation(maybeClone(transformation))
    , m_cesiumSettings(maybeClone(cesiumSettings))
    , m_version(makeUnique<Version>(currentVersion()))
    , m_srs()
    , m_errors()
{ }

Metadata::Metadata(const arbiter::Endpoint& ep, const std::size_t* subsetId)
    : Metadata(([&ep, subsetId]()
    {
        // Prior to 1.0, there were some keys nested in the top-level "format"
        // key.  Now those nested keys are themselves at the top level.
        Json::Value json(parse(ep.get("entwine" + getPostfix(subsetId))));
        if (json.isMember("format"))
        {
            for (const auto& k : json["format"].getMemberNames())
            {
                json[k] = json["format"][k];
            }
        }
        return json;
    })())
{
    const Json::Value json(
            parse(ep.get("entwine-manifest" + getPostfix(subsetId))));

    m_manifest = makeUnique<Manifest>(json, ep);
}

Metadata::Metadata(const Json::Value& json)
    : m_boundsNative(
            makeUnique<Bounds>(json.isMember("boundsNative") ?
                json["boundsNative"] : json["boundsConforming"]))
    , m_boundsConforming(makeUnique<Bounds>(json["boundsConforming"]))
    , m_boundsEpsilon(makeUnique<Bounds>(m_boundsConforming->growBy(epsilon)))
    , m_bounds(makeUnique<Bounds>(json["bounds"]))
    , m_schema(makeUnique<Schema>(json["schema"]))
    , m_structure(makeUnique<Structure>(json["structure"]))
    , m_hierarchyStructure(makeUnique<Structure>(json["hierarchyStructure"]))
    , m_manifest()
    , m_delta(Delta::maybeCreate(json))
    , m_format(makeUnique<Format>(*this, json))
    , m_reprojection(maybeCreate<Reprojection>(json["reprojection"]))
    , m_subset(json.isMember("subset") ?
            makeUnique<Subset>(*m_bounds, json["subset"]) : nullptr)
    , m_transformation(json.isMember("transformation") ?
            makeUnique<Transformation>() : nullptr)
    , m_cesiumSettings(
            json.isMember("formats") && json["formats"].isMember("cesium") ?
                makeUnique<cesium::Settings>(json["formats"]["cesium"]) :
                nullptr)
    , m_version(makeUnique<Version>(json["version"].asString()))
    , m_srs(json["srs"].asString())
    , m_errors(fromJsonArray(json["errors"]))
{
    if (m_transformation)
    {
        for (const auto& v : json["transformation"])
        {
            m_transformation->push_back(v.asDouble());
        }
    }
}

Metadata::Metadata(const Metadata& other)
    : m_boundsNative(makeUnique<Bounds>(other.boundsNative()))
    , m_boundsConforming(makeUnique<Bounds>(other.boundsConforming()))
    , m_boundsEpsilon(makeUnique<Bounds>(other.boundsEpsilon()))
    , m_bounds(makeUnique<Bounds>(other.bounds()))
    , m_schema(makeUnique<Schema>(other.schema()))
    , m_structure(makeUnique<Structure>(other.structure()))
    , m_hierarchyStructure(makeUnique<Structure>(other.hierarchyStructure()))
    , m_manifest(makeUnique<Manifest>(other.manifest()))
    , m_delta(maybeClone(other.delta()))
    , m_format(makeUnique<Format>(*this, other.format()))
    , m_reprojection(maybeClone(other.reprojection()))
    , m_subset(maybeClone(other.subset()))
    , m_transformation(maybeClone(other.transformation()))
    , m_cesiumSettings(maybeClone(other.cesiumSettings()))
    , m_version(makeUnique<Version>(other.version()))
    , m_srs(other.srs())
    , m_errors(other.errors())
{ }

Metadata::~Metadata() { }

Json::Value Metadata::toJson() const
{
    Json::Value json;

    json["boundsNative"] = m_boundsNative->toJson();
    json["boundsConforming"] = m_boundsConforming->toJson();
    json["bounds"] = m_bounds->toJson();
    json["schema"] = m_schema->toJson();
    json["structure"] = m_structure->toJson();
    json["hierarchyStructure"] = m_hierarchyStructure->toJson();

    const Json::Value format(m_format->toJson());
    for (const auto& k : format.getMemberNames()) json[k] = format[k];

    if (m_srs.size()) json["srs"] = m_srs;
    if (m_reprojection) json["reprojection"] = m_reprojection->toJson();
    if (m_subset) json["subset"] = m_subset->toJson();

    if (m_delta)
    {
        json["scale"] = m_delta->scale().toJsonArray();
        json["offset"] = m_delta->offset().toJsonArray();
    }

    if (m_transformation)
    {
        for (const double v : *m_transformation)
        {
            json["transformation"].append(v);
        }
    }

    if (m_cesiumSettings)
    {
        json["formats"]["cesium"] = m_cesiumSettings->toJson();
    }

    if (m_errors.size())
    {
        for (const auto& e : m_errors) json["errors"].append(e);
    }

    json["version"] = m_version->toString();

    return json;
}

void Metadata::save(const arbiter::Endpoint& endpoint) const
{
    const auto json(toJson());
    Storage::ensurePut(endpoint, "entwine" + postfix(), json.toStyledString());
    if (m_manifest) m_manifest->save(postfix());
}

void Metadata::merge(const Metadata& other)
{
    if (m_srs.empty()) m_srs = other.srs();
    m_manifest->merge(other.manifest());
}

std::string Metadata::postfix(const bool isColdChunk) const
{
    // Things we save, and their postfixing.
    //
    // Metadata files (main meta, ids, manifest):
    //      All postfixes applied.
    //
    // Base (both data/hierarchy) chunk:
    //      All postfixes applied.
    //
    // Cold hierarchy chunks:
    //      All postfixes applied.
    //
    // Cold data chunks:
    //      No subset postfixing.
    //
    // Hierarchy metadata:
    //      All postfixes applied.
    std::string pf;

    if (m_subset && !isColdChunk) pf += m_subset->postfix();

    return pf;
}

void Metadata::makeWhole()
{
    m_subset.reset();
    m_structure->unbump();
    m_hierarchyStructure->unbump();
}

const Bounds* Metadata::boundsSubset() const
{
    return m_subset ? &m_subset->bounds() : nullptr;
}

} // namespace entwine


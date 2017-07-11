/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <cassert>

#include <entwine/formats/cesium/settings.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/manifest.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/storage.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    const double epsilon(0.005);
}

Metadata::Metadata(
        const Bounds& boundsNativeConforming,
        const Schema& schema,
        const Structure& structure,
        const Structure& hierarchyStructure,
        const Manifest& manifest,
        const bool trustHeaders,
        const ChunkStorageType chunkStorage,
        const HierarchyCompression hierarchyCompress,
        const double density,
        const Reprojection* reprojection,
        const Subset* subset,
        const Delta* delta,
        const Transformation* transformation,
        const cesium::Settings* cesiumSettings)
    : m_delta(maybeClone(delta))
    , m_boundsNativeConforming(clone(boundsNativeConforming))
    , m_boundsNativeCubic(clone(makeNativeCube(boundsNativeConforming, delta)))
    , m_boundsScaledConforming(
            clone(m_boundsNativeConforming->deltify(m_delta.get())))
    , m_boundsScaledCubic(
            clone(m_boundsNativeConforming->cubeify(m_delta.get())))
    , m_boundsScaledEpsilon(
            clone(m_boundsScaledConforming->growBy(epsilon)))
    , m_schema(makeUnique<Schema>(schema))
    , m_structure(makeUnique<Structure>(structure))
    , m_hierarchyStructure(makeUnique<Structure>(hierarchyStructure))
    , m_manifest(makeUnique<Manifest>(manifest))
    , m_storage(makeUnique<Storage>(*this, chunkStorage, hierarchyCompress))
    , m_reprojection(maybeClone(reprojection))
    , m_subset(maybeClone(subset))
    , m_transformation(maybeClone(transformation))
    , m_cesiumSettings(maybeClone(cesiumSettings))
    , m_version(makeUnique<Version>(currentVersion()))
    , m_srs(m_reprojection ? m_reprojection->out() : "")
    , m_density(density)
    , m_trustHeaders(trustHeaders)
{
    if (!m_density) m_density = densityLowerBound(*m_manifest);
}

Metadata::Metadata(const arbiter::Endpoint& ep, const std::size_t* subsetId)
    : Metadata(([&ep, subsetId]()
    {
        // Note that we are not fully-constructed yet so we can't call our
        // Metadata::postfix() yet, as we would like to.
        const std::string path("entwine" + Subset::postfix(subsetId));
        Json::Value json = subsetId ?
            parse(io::ensureGetString(ep, path)) :
            parse(ep.get(path));

        // Pre-1.0: nested keys have since been flattened.
        if (json.isMember("format"))
        {
            for (const auto& k : json["format"].getMemberNames())
            {
                json[k] = json["format"][k];
            }
        }

        // Pre-1.0: casing was inconsistent with other keys.
        if (json.isMember("compress-hierarchy"))
        {
            json["compressHierarchy"] = json["compress-hierarchy"];
        }

        // 1.0: storage was a boolean named "compress", and only lazperf and
        // uncompressed-binary was supported.
        if (json.isMember("compress"))
        {
            json["storage"] = json["compress"].asBool() ? "lazperf" : "binary";
        }

        return json;
    })())
{
    assert(!subsetId || *subsetId == m_subset->id());
    const std::string path("entwine-manifest" + postfix());
    const Json::Value json(parse(io::ensureGetString(ep, path)));
    m_manifest = makeUnique<Manifest>(json, ep);
    if (!m_density) m_density = densityLowerBound(*m_manifest);
}

Metadata::Metadata(const Json::Value& json)
    : m_delta(Delta::maybeCreate(json))
    , m_boundsNativeConforming(makeUnique<Bounds>(json["boundsConforming"]))
    , m_boundsNativeCubic(makeUnique<Bounds>(json["bounds"]))
    , m_boundsScaledConforming(
            clone(m_boundsNativeConforming->deltify(m_delta.get())))
    , m_boundsScaledCubic(
            clone(m_boundsNativeConforming->cubeify(m_delta.get())))
    , m_boundsScaledEpsilon(
            clone(m_boundsScaledConforming->growBy(epsilon)))
    , m_schema(makeUnique<Schema>(json["schema"]))
    , m_structure(makeUnique<Structure>(json["structure"]))
    , m_hierarchyStructure(makeUnique<Structure>(json["hierarchyStructure"]))
    , m_manifest()
    , m_storage(makeUnique<Storage>(*this, json))
    , m_reprojection(maybeCreate<Reprojection>(json["reprojection"]))
    , m_subset(json.isMember("subset") ?
            makeUnique<Subset>(boundsNativeCubic(), json["subset"]) : nullptr)
    , m_transformation(json.isMember("transformation") ?
            makeUnique<Transformation>(
                extract<double>(json["transformation"])) :
            nullptr)
    , m_cesiumSettings(
            json.isMember("formats") && json["formats"].isMember("cesium") ?
                makeUnique<cesium::Settings>(json["formats"]["cesium"]) :
                nullptr)
    , m_version(makeUnique<Version>(json["version"].asString()))
    , m_srs(json["srs"].asString())
    , m_density(json["density"].asDouble())
    , m_trustHeaders(json["trustHeaders"].asBool())
    , m_slicedBase(json["baseType"].asString() == "sliced")
{ }

Metadata::Metadata(const Metadata& other)
    : m_delta(maybeClone(other.delta()))
    , m_boundsNativeConforming(clone(other.boundsNativeConforming()))
    , m_boundsNativeCubic(clone(other.boundsNativeCubic()))
    , m_boundsScaledConforming(clone(other.boundsScaledConforming()))
    , m_boundsScaledCubic(clone(other.boundsScaledCubic()))
    , m_boundsScaledEpsilon(clone(other.boundsScaledEpsilon()))
    , m_schema(makeUnique<Schema>(other.schema()))
    , m_structure(makeUnique<Structure>(other.structure()))
    , m_hierarchyStructure(makeUnique<Structure>(other.hierarchyStructure()))
    , m_manifest(makeUnique<Manifest>(other.manifest()))
    , m_storage(makeUnique<Storage>(*this, other.storage()))
    , m_reprojection(maybeClone(other.reprojection()))
    , m_subset(maybeClone(other.subset()))
    , m_transformation(maybeClone(other.transformation()))
    , m_cesiumSettings(maybeClone(other.cesiumSettings()))
    , m_version(makeUnique<Version>(other.version()))
    , m_srs(other.srs())
    , m_density(other.density())
    , m_trustHeaders(other.trustHeaders())
    , m_slicedBase(other.slicedBase())
{ }

Metadata::~Metadata() { }

Json::Value Metadata::toJson() const
{
    Json::Value json;

    json["bounds"] = boundsNativeCubic().toJson();
    json["boundsConforming"] = boundsNativeConforming().toJson();
    json["schema"] = m_schema->toJson();
    json["structure"] = m_structure->toJson();
    json["hierarchyStructure"] = m_hierarchyStructure->toJson();
    json["trustHeaders"] = m_trustHeaders;

    const Json::Value storage(m_storage->toJson());
    for (const auto& k : storage.getMemberNames()) json[k] = storage[k];

    if (m_srs.size()) json["srs"] = m_srs;
    if (m_reprojection) json["reprojection"] = m_reprojection->toJson();
    if (m_subset) json["subset"] = m_subset->toJson();

    if (m_delta) m_delta->insertInto(json);
    if (m_slicedBase) json["baseType"] = "sliced";

    if (m_transformation)
    {
        for (const double v : *m_transformation)
        {
            json["transformation"].append(v);
        }
    }

    if (m_density) json["density"] = m_density;

    if (m_cesiumSettings)
    {
        json["formats"]["cesium"] = m_cesiumSettings->toJson();
    }

    json["version"] = m_version->toString();

    return json;
}

void Metadata::save(const arbiter::Endpoint& endpoint) const
{
    const auto json(toJson());
    io::ensurePut(endpoint, "entwine" + postfix(), json.toStyledString());

    const bool primary(!m_subset || m_subset->primary());
    if (m_manifest) m_manifest->save(primary, postfix());
}

void Metadata::merge(const Metadata& other)
{
    if (m_srs.empty()) m_srs = other.srs();
    m_manifest->merge(other.manifest());
}

std::string Metadata::basename(const Id& chunkId) const
{
    return
        m_structure->maybePrefix(chunkId) +
        postfix(chunkId >= m_structure->coldIndexBegin());
}

std::string Metadata::filename(const Id& chunkId) const
{
    return m_storage->filename(chunkId);
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
    return m_subset && !isColdChunk ? m_subset->postfix() : "";
}

void Metadata::makeWhole()
{
    m_subset.reset();
    m_structure->unbump();
    m_hierarchyStructure->unbump(false);
}

void Metadata::unbump()
{
    m_structure->unbump();
}

std::unique_ptr<Bounds> Metadata::boundsNativeSubset() const
{
    return m_subset ? clone(m_subset->bounds()) : nullptr;
}

std::unique_ptr<Bounds> Metadata::boundsScaledSubset() const
{
    return m_subset ? clone(m_subset->bounds().deltify(delta())) : nullptr;
}

Bounds Metadata::makeScaledCube(
        const Bounds& nativeConformingBounds,
        const Delta* delta)
{
    return nativeConformingBounds.cubeify(delta);
}

Bounds Metadata::makeNativeCube(
        const Bounds& nativeConformingBounds,
        const Delta* delta)
{
    return makeScaledCube(nativeConformingBounds, delta).undeltify(delta);
}

} // namespace entwine


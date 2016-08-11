/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/manifest.hpp>
#include <entwine/types/format.hpp>
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
}

Metadata::Metadata(
        const Bounds& boundsConforming,
        const Schema& schema,
        const Structure& structure,
        const Structure& hierarchyStructure,
        const Manifest& manifest,
        const Format& format,
        const Reprojection* reprojection,
        const Subset* subset)
    : m_boundsConforming(makeUnique<Bounds>(boundsConforming))
    , m_boundsEpsilon(makeUnique<Bounds>(m_boundsConforming->growBy(epsilon)))
    , m_bounds(makeUnique<Bounds>(m_boundsConforming->cubeify()))
    , m_schema(makeUnique<Schema>(schema))
    , m_structure(makeUnique<Structure>(structure))
    , m_hierarchyStructure(makeUnique<Structure>(hierarchyStructure))
    , m_manifest(makeUnique<Manifest>(manifest))
    , m_format(makeUnique<Format>(format))
    , m_reprojection(maybeClone(reprojection))
    , m_subset(maybeClone(subset))
    , m_errors()
{ }

Metadata::Metadata(
        const arbiter::Endpoint& ep,
        const std::size_t* subsetId,
        const std::size_t* splitId)
{
    const std::string pf(
            (subsetId ? "-" + std::to_string(*subsetId) : "") +
            (splitId  ? "-" + std::to_string(*splitId)  : ""));

    const Json::Value meta(parse(ep.get("entwine" + pf)));
    const Json::Value manifest(parse(ep.get("entwine-manifest" + pf)));

    m_boundsConforming = makeUnique<Bounds>(meta["boundsConforming"]);
    m_boundsEpsilon = makeUnique<Bounds>(m_boundsConforming->growBy(epsilon));
    m_bounds = makeUnique<Bounds>(meta["bounds"]);
    m_schema = makeUnique<Schema>(meta["schema"]);
    m_structure = makeUnique<Structure>(meta["structure"]);
    m_hierarchyStructure = makeUnique<Structure>(meta["hierarchyStructure"]);

    m_manifest = makeUnique<Manifest>(manifest);
    m_format = makeUnique<Format>(*m_schema, meta["format"]);

    if (meta.isMember("reprojection"))
    {
        m_reprojection = makeUnique<Reprojection>(meta["reprojection"]);
    }

    if (meta.isMember("subset"))
    {
        m_subset = makeUnique<Subset>(*m_bounds, meta["subset"]);
    }

    if (meta.isMember("errors"))
    {
        m_errors = fromJsonArray(meta["errors"]);
    }
}

Metadata::Metadata(const Json::Value& json)
    : m_boundsConforming(makeUnique<Bounds>(json["boundsConforming"]))
    , m_boundsEpsilon(makeUnique<Bounds>(m_boundsConforming->growBy(epsilon)))
    , m_bounds(makeUnique<Bounds>(json["bounds"]))
    , m_schema(makeUnique<Schema>(json["schema"]))
    , m_structure(makeUnique<Structure>(json["structure"]))
    , m_hierarchyStructure(makeUnique<Structure>(json["hierarchyStructure"]))
    , m_manifest()
    , m_format(makeUnique<Format>(*m_schema, json["format"]))
    , m_reprojection(json.isMember("reprojection") ?
            makeUnique<Reprojection>(json["reprojection"]) : nullptr)
    , m_subset(json.isMember("subset") ?
            makeUnique<Subset>(*m_bounds, json["subset"]) : nullptr)
    , m_errors(fromJsonArray(json["errors"]))
{ }

Metadata::Metadata(const Metadata& other)
    : m_boundsConforming(makeUnique<Bounds>(other.boundsConforming()))
    , m_boundsEpsilon(makeUnique<Bounds>(other.boundsEpsilon()))
    , m_bounds(makeUnique<Bounds>(other.bounds()))
    , m_schema(makeUnique<Schema>(other.schema()))
    , m_structure(makeUnique<Structure>(other.structure()))
    , m_hierarchyStructure(makeUnique<Structure>(other.hierarchyStructure()))
    , m_manifest(makeUnique<Manifest>(other.manifest()))
    , m_format(makeUnique<Format>(other.format()))
    , m_reprojection(maybeClone(other.reprojection()))
    , m_subset(maybeClone(other.subset()))
    , m_errors(other.errors())
{ }

Metadata::~Metadata() { }

void Metadata::save(const arbiter::Endpoint& endpoint) const
{
    Json::Value json;

    json["boundsConforming"] = m_boundsConforming->toJson();
    json["bounds"] = m_bounds->toJson();
    json["schema"] = m_schema->toJson();
    json["structure"] = m_structure->toJson();
    json["hierarchyStructure"] = m_hierarchyStructure->toJson();
    json["format"] = m_format->toJson();

    if (m_reprojection) json["reprojection"] = m_reprojection->toJson();
    if (m_subset) json["subset"] = m_subset->toJson();

    if (m_errors.size())
    {
        for (const auto& e : m_errors) json["errors"].append(e);
    }

    const auto pf(postfix());
    Storage::ensurePut(endpoint, "entwine" + pf, json.toStyledString());

    if (m_manifest)
    {
        const std::string manifestContents(
                m_manifest->size() < 500 ?
                    m_manifest->toJson().toStyledString() :
                    toFastString(m_manifest->toJson()));

        Storage::ensurePut(endpoint, "entwine-manifest" + pf, manifestContents);
    }
}

void Metadata::merge(const Metadata& other)
{
    if (m_format->srs().empty()) m_format->srs() = other.format().srs();
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
    //      Split postfixing applied to splits except for the nominal split.
    //
    // Hierarchy metadata:
    //      All postfixes applied.
    std::string pf;

    if (m_subset && !isColdChunk) pf += m_subset->postfix();

    if (m_manifest)
    {
        if (const Manifest::Split* split = m_manifest->split())
        {
            if (split->begin() || !isColdChunk) pf += split->postfix();
        }
    }

    return pf;
}

void Metadata::makeWhole()
{
    m_subset.reset();
    m_structure->unbump();
    m_manifest->unsplit();
}

const Bounds* Metadata::boundsSubset() const
{
    return m_subset ? &m_subset->bounds() : nullptr;
}

} // namespace entwine


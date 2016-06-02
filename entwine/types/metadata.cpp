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
#include <entwine/types/metadata.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Metadata::Metadata(
        const BBox& bboxConforming,
        const Schema& schema,
        const Structure& structure,
        const Manifest& manifest,
        const Reprojection* reprojection,
        const Subset* subset,
        const bool trustHeaders,
        const bool compress)
    : m_bboxConforming(makeUnique<BBox>(bboxConforming))
    , m_bboxEpsilon(makeUnique<BBox>(m_bboxConforming->growBy(0.005)))
    , m_bbox(makeUnique<BBox>(m_bboxConforming->cubeify()))
    , m_schema(makeUnique<Schema>(schema))
    , m_structure(makeUnique<Structure>(structure))
    , m_manifest(makeUnique<Manifest>(manifest))
    , m_reprojection(maybeClone(reprojection))
    , m_subset(maybeClone(subset))
    , m_srs()
    , m_trustHeaders(trustHeaders)
    , m_compress(compress)
    , m_errors()
{ }

Metadata::Metadata(const arbiter::Endpoint& ep, const std::size_t* subsetId)
{
    const std::string pf(subsetId ? "-" + std::to_string(*subsetId) : "");

    const Json::Value meta(parse(ep.get("entwine" + pf)));
    const Json::Value manifest(parse(ep.get("entwine-manifest" + pf)));

    m_bboxConforming = makeUnique<BBox>(meta["bboxConforming"]);
    m_bboxEpsilon = makeUnique<BBox>(m_bboxConforming->growBy(0.005));
    m_bbox = makeUnique<BBox>(meta["bbox"]);
    m_schema = makeUnique<Schema>(meta["schema"]);
    m_structure = makeUnique<Structure>(meta["structure"]);

    m_manifest = makeUnique<Manifest>(manifest);

    m_srs = meta["srs"].asString();
    m_trustHeaders = meta["trustHeaders"].asBool();
    m_compress = meta["compressed"].asBool();

    if (meta.isMember("reprojection"))
    {
        m_reprojection = makeUnique<Reprojection>(meta["reprojection"]);
    }

    if (meta.isMember("subset"))
    {
        m_subset = makeUnique<Subset>(*m_bbox, meta["subset"]);
    }

    if (meta.isMember("errors"))
    {
        const Json::Value& errors(meta["errors"]);

        for (Json::ArrayIndex i(0); i < errors.size(); ++i)
        {
            m_errors.push_back(errors[i].asString());
        }
    }
}

Metadata::Metadata(const Metadata& other)
    : m_bboxConforming(makeUnique<BBox>(other.bboxConforming()))
    , m_bboxEpsilon(makeUnique<BBox>(other.bboxEpsilon()))
    , m_bbox(makeUnique<BBox>(other.bbox()))
    , m_schema(makeUnique<Schema>(other.schema()))
    , m_structure(makeUnique<Structure>(other.structure()))
    , m_manifest(makeUnique<Manifest>(other.manifest()))
    , m_reprojection(maybeClone(other.reprojection()))
    , m_subset(maybeClone(other.subset()))
    , m_srs(other.srs())
    , m_trustHeaders(other.trustHeaders())
    , m_compress(other.compress())
    , m_errors(other.errors())
{ }

void Metadata::save(const arbiter::Endpoint& endpoint) const
{
    Json::Value json;

    json["bboxConforming"] = m_bboxConforming->toJson();
    json["bbox"] = m_bbox->toJson();
    json["schema"] = m_schema->toJson();
    json["structure"] = m_structure->toJson();
    json["srs"] = m_srs;
    json["trustHeaders"] = m_trustHeaders;
    json["compressed"] = m_compress;

    if (m_reprojection) json["reprojection"] = m_reprojection->toJson();
    if (m_subset) json["subset"] = m_subset->toJson();

    if (m_errors.size())
    {
        for (const auto& e : m_errors) json["errors"].append(e);
    }

    const auto pf(postfix());
    endpoint.put("entwine" + pf, json.toStyledString());
    endpoint.put("entwine-manifest" + pf, toFastString(m_manifest->toJson()));
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
    // Base chunk:
    //      All postfixes applied.
    //
    // Other chunks:
    //      No subset postfixing.
    //      Split postfixing applied to splits except for the nominal split.
    //
    // Hierarchy:
    //      All postfixes applied.
    std::string pf;

    if (m_subset && !isColdChunk) pf += m_subset->postfix();

    if (const Manifest::Split* split = m_manifest->split())
    {
        if (split->begin() || !isColdChunk) pf += split->postfix();
    }

    return pf;
}

void Metadata::makeWhole()
{
    m_subset.reset();
    m_manifest->unsplit();
}

const BBox& Metadata::bboxConforming() const { return *m_bboxConforming; }
const BBox& Metadata::bboxEpsilon() const { return *m_bboxEpsilon; }
const BBox& Metadata::bbox() const { return *m_bbox; }

const BBox* Metadata::bboxSubset() const
{
    return m_subset ? &m_subset->bbox() : nullptr;
}

const Schema& Metadata::schema() const { return *m_schema; }
const Structure& Metadata::structure() const { return *m_structure; }
const Manifest& Metadata::manifest() const { return *m_manifest; }

/*
const Reprojection* Metadata::reprojection() const
{
    return m_reprojection.get();
}

const Subset* Metadata::subset() const
{
    return m_subset.get();
}
*/

/*
BBox& Metadata::bboxConforming() { return *m_bboxConforming; }
BBox& Metadata::bbox() { return *m_bbox; }
Schema& Metadata::schema() { return *m_schema; }
Structure& Metadata::structure() { return *m_structure; }
*/
Manifest& Metadata::manifest() { return *m_manifest; }

} // namespace entwine


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

#include <entwine/types/chunk-storage/chunk-storage.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/manifest.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Metadata::Metadata(const Config& config)
    : m_delta(config.delta().exists() ?
            makeUnique<Delta>(config.delta()) : std::unique_ptr<Delta>())
    , m_boundsNativeConforming(makeUnique<Bounds>(
                makeNativeConformingBounds(
                    config.boundsConforming(),
                    config.delta())))
    , m_boundsNativeCubic(
            config.json().isMember("bounds") &&
            config.json().isMember("boundsConforming") ?
                makeUnique<Bounds>(config["bounds"]) :
                clone(makeNativeCube(*m_boundsNativeConforming, m_delta.get())))
    , m_boundsScaledConforming(
            clone(m_boundsNativeConforming->deltify(m_delta.get())))
    , m_boundsScaledCubic(
            clone(m_boundsNativeConforming->cubeify(m_delta.get())))
    , m_schema(makeUnique<Schema>(config["schema"]))
    , m_subset(Subset::create(*this, config["subset"]))
    , m_files(makeUnique<Files>(config.input()))
    , m_structure(makeUnique<NewStructure>(*this, config.json()))
    , m_chunkStorage(ChunkStorage::create(*this, config))
    , m_reprojection(Reprojection::create(config["reprojection"]))
    , m_version(makeUnique<Version>(currentVersion()))
    , m_srs(config.srs())
    , m_density(config.density())
    , m_trustHeaders(config.trustHeaders())
{
    if (m_srs.empty() && m_reprojection) m_srs = m_reprojection->out();
}

Metadata::Metadata(const arbiter::Endpoint& ep, const Config& config)
    : Metadata(
            entwine::merge(
                config.json(),
                parse(ep.get("entwine" + config.postfix() + ".json"))))
{
    Files files(parse(ep.get("entwine-files" + postfix() + ".json")));
    files.append(m_files->list());
    m_files = makeUnique<Files>(files.list());
}

Metadata::~Metadata() { }

Json::Value Metadata::toJson() const
{
    Json::Value json;

    json["bounds"] = boundsNativeCubic().toJson();
    json["boundsConforming"] = boundsNativeConforming().toJson();
    json["schema"] = m_schema->toJson();
    json["structure"] = m_structure->toJson();
    json["numPoints"] = Json::UInt64(m_structure->numPointsHint());
    json["trustHeaders"] = m_trustHeaders;

    /*
    const Json::Value storage(m_chunkStorage->toJson());
    for (const auto& k : storage.getMemberNames()) json[k] = storage[k];
    */

    if (m_srs.size()) json["srs"] = m_srs;
    if (m_reprojection) json["reprojection"] = m_reprojection->toJson();
    // if (m_subset) json["subset"] = m_subset->toJson();

    if (m_delta) m_delta->insertInto(json);

    if (m_transformation)
    {
        for (const double v : *m_transformation)
        {
            json["transformation"].append(v);
        }
    }

    if (m_density) json["density"] = m_density;

    json["version"] = m_version->toString();
    json["dataStorage"] = "laszip";
    json["hierarchyStorage"] = "json";

    for (const auto s : m_preserveSpatial) json["preserveSpatial"].append(s);

    return json;
}

void Metadata::save(const arbiter::Endpoint& endpoint) const
{
    const auto json(toJson());
    const std::string f("entwine" + postfix() + ".json");
    io::ensurePut(endpoint, f, json.toStyledString());

    m_files->save(endpoint, postfix());
    /*
    const bool primary(!m_subset || m_subset->primary());
    if (m_manifest) m_manifest->save(primary, postfix());
    */
}

void Metadata::merge(const Metadata& other)
{
    if (m_srs.empty()) m_srs = other.srs();
    // m_manifest->merge(other.manifest());
}

void Metadata::makeWhole()
{
    // m_subset.reset();
    // m_structure->unbump();
}

void Metadata::unbump()
{
    // m_structure->unbump();
}

/*
std::unique_ptr<Bounds> Metadata::boundsNativeSubset() const
{
    return m_subset ? clone(m_subset->bounds()) : nullptr;
}

std::unique_ptr<Bounds> Metadata::boundsScaledSubset() const
{
    return m_subset ? clone(m_subset->bounds().deltify(delta())) : nullptr;
}
*/

std::string Metadata::postfix() const
{
    if (const Subset* s = subset()) return "-" + std::to_string(s->id());
    return "";
}

std::string Metadata::postfix(const uint64_t depth) const
{
    if (const Subset* s = subset())
    {
        if (depth < m_structure->minTail())
        {
            return "-" + std::to_string(s->id());
        }
    }

    return "";
}

Bounds Metadata::makeNativeConformingBounds(const Bounds& b) const
{
    Point pmin(b.min());
    Point pmax(b.max());

    pmin.apply([](double d)
    {
        if (static_cast<double>(static_cast<uint64_t>(d)) == d) return d - 1.0;
        else return std::floor(d);
    });

    pmax.apply([](double d)
    {
        if (static_cast<double>(static_cast<uint64_t>(d)) == d) return d + 1.0;
        else return std::ceil(d);
    });

    return Bounds(pmin, pmax);
}

Bounds Metadata::makeNativeCube(const Bounds& b, const Delta& d) const
{
    const double maxDist(std::max(std::max(b.width(), b.depth()), b.height()));
    double radius(std::ceil(maxDist / 2.0) + 1.0);

    const Scale& s(d.scale());
    const Point p(radius / s.x, radius / s.y, radius / s.z);

    return Bounds(-p, p);
}

} // namespace entwine


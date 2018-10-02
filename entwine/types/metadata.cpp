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

#include <entwine/io/io.hpp>
#include <entwine/types/files.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/srs.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Metadata::Metadata(const Config& config, const bool exists)
    : m_outSchema(makeUnique<Schema>(config.schema()))
    , m_schema(makeUnique<Schema>(Schema::makeAbsolute(*m_outSchema)))
    , m_boundsConforming(makeUnique<Bounds>(
                exists ?
                    Bounds(config["boundsConforming"]) :
                    makeConformingBounds(config["bounds"])))
    , m_boundsCubic(makeUnique<Bounds>(
                exists ?
                    Bounds(config["bounds"]) :
                    makeCube(*m_boundsConforming)))
    , m_files(makeUnique<Files>(config.input()))
    , m_dataIo(DataIo::create(*this, config.dataType()))
    , m_reprojection(Reprojection::create(config["reprojection"]))
    , m_version(makeUnique<Version>(currentVersion()))
    , m_srs(makeUnique<Srs>(config.srs()))
    , m_subset(Subset::create(*this, config["subset"]))
    , m_trustHeaders(config.trustHeaders())
    , m_ticks(config.ticks())
    , m_startDepth(std::log2(m_ticks))
    , m_sharedDepth(m_subset ? m_subset->splits() : 0)
    , m_overflowDepth(std::max(config.overflowDepth(), m_sharedDepth))
    , m_overflowThreshold(config.overflowThreshold())
    , m_hierarchyStep(config.hierarchyStep())
{
    if (1UL << m_startDepth != m_ticks)
    {
        throw std::runtime_error("Invalid 'ticks' setting");
    }

    if (m_outSchema->isScaled())
    {
        const Scale scale(m_outSchema->scale());
        const Offset offset(m_outSchema->offset());

        const uint64_t size = std::min(
                m_outSchema->find("X").size(),
                std::min(
                    m_outSchema->find("Y").size(),
                    m_outSchema->find("Z").size()));
        Point mn;
        Point mx;

        if (size == 4)
        {
            mn = std::numeric_limits<int32_t>::lowest();
            mx = std::numeric_limits<int32_t>::max();
        }
        else if (size == 8)
        {
            mn = std::numeric_limits<int64_t>::lowest();
            mx = std::numeric_limits<int64_t>::max();
        }

        const Bounds extents(mn, mx);
        const Bounds request(m_boundsCubic->applyScaleOffset(scale, offset));

        if (!extents.contains(request))
        {
            std::cout <<
                "Maximal extents: " << extents << "\n" <<
                "Scaled bounds:   " << request << std::endl;
            throw std::runtime_error(
                    "Bounds are too large for the selected scale");
        }
    }
}

Metadata::Metadata(const arbiter::Endpoint& ep, const Config& config)
    : Metadata(
            entwine::merge(
                config.json(),
                entwine::merge(
                    parse(ep.get("ept" + config.postfix() + ".json")),
                    parse(ep.get("ept-build" + config.postfix() + ".json")))),
            true)
{
    Files files(parse(ep.get("ept-files" + postfix() + ".json")));
    files.append(m_files->list());
    m_files = makeUnique<Files>(files.list());
}

Metadata::~Metadata() { }

Json::Value Metadata::toJson() const
{
    Json::Value json;

    json["version"] = m_version->toString();
    json["bounds"] = boundsCubic().toJson();
    json["boundsConforming"] = boundsConforming().toJson();
    json["schema"] = m_outSchema->toJson();
    json["ticks"] = (Json::UInt64)m_ticks;
    json["numPoints"] = (Json::UInt64)m_files->totalPoints();
    json["dataType"] = m_dataIo->type();
    json["hierarchyType"] = "json"; // TODO.

    if (m_srs->exists()) json["srs"] = m_srs->toJson();
    if (m_hierarchyStep) json["hierarchyStep"] = (Json::UInt64)m_hierarchyStep;

    return json;
}

Json::Value Metadata::toBuildParamsJson() const
{
    Json::Value json;

    json["trustHeaders"] = m_trustHeaders;
    json["overflowDepth"] = (Json::UInt64)m_overflowDepth;
    json["overflowThreshold"] = (Json::UInt64)m_overflowThreshold;
    if (m_subset) json["subset"] = m_subset->toJson();
    if (m_reprojection) json["reprojection"] = m_reprojection->toJson();

    return json;
}

void Metadata::save(const arbiter::Endpoint& endpoint, const Config& config)
    const
{
    {
        const auto json(toJson());
        const std::string f("ept" + postfix() + ".json");
        ensurePut(endpoint, f, toPreciseString(json));
    }

    {
        const auto json(toBuildParamsJson());
        const std::string f("ept-build" + postfix() + ".json");
        ensurePut(endpoint, f, toPreciseString(json));
    }

    const bool detailed(
            !m_merged &&
            primary() &&
            (!config.json().isMember("writeDetails") ||
                config["writeDetails"].asBool()));
    m_files->save(endpoint, postfix(), config, detailed);
}

void Metadata::merge(const Metadata& other)
{
    m_files->merge(other.files());
}

void Metadata::makeWhole()
{
    m_merged = true;
    m_subset.reset();
}

std::string Metadata::postfix() const
{
    if (const Subset* s = subset()) return "-" + std::to_string(s->id());
    return "";
}

std::string Metadata::postfix(const uint64_t depth) const
{
    if (const Subset* s = subset())
    {
        if (depth < m_sharedDepth)
        {
            return "-" + std::to_string(s->id());
        }
    }

    return "";
}

Bounds Metadata::makeConformingBounds(const Bounds& b) const
{
    Point pmin(b.min());
    Point pmax(b.max());

    pmin = pmin.apply([](double d)
    {
        if (std::floor(d) == d) return d - 1.0;
        else return std::floor(d);
    });

    pmax = pmax.apply([](double d)
    {
        if (std::ceil(d) == d) return d + 1.0;
        else return std::ceil(d);
    });

    return Bounds(pmin, pmax);
}

Bounds Metadata::makeCube(const Bounds& b) const
{
    double diam(std::max(std::max(b.width(), b.depth()), b.height()));
    double r(std::ceil(diam / 2.0) + 1.0);

    const Point mid(b.mid().apply([](double d) { return std::round(d); }));
    return Bounds(mid - r, mid + r);
}

} // namespace entwine


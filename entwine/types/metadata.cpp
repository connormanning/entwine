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
#include <entwine/types/delta.hpp>
#include <entwine/types/files.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Metadata::Metadata(const Config& config, const bool exists)
    : m_delta(config.delta())
    , m_boundsNativeConforming(makeUnique<Bounds>(
                exists ?
                    Bounds(config["boundsConforming"]) :
                    makeNativeConformingBounds(config["bounds"])))
    , m_boundsNativeCubic(makeUnique<Bounds>(
                exists ?
                    Bounds(config["bounds"]) :
                    makeNativeCube(*m_boundsNativeConforming, m_delta.get())))
    , m_boundsScaledConforming(
            clone(m_boundsNativeConforming->deltify(m_delta.get())))
    , m_boundsScaledCubic(
            clone(m_boundsNativeCubic->deltify(m_delta.get())))
    , m_schema(makeUnique<Schema>(config.schema()))
    , m_files(makeUnique<Files>(config.input()))
    , m_dataIo(DataIo::create(*this, config.dataType()))
    , m_reprojection(Reprojection::create(config["reprojection"]))
    , m_version(makeUnique<Version>(currentVersion()))
    , m_srs(config.srs().empty() && m_reprojection ?
            m_reprojection->out() : config.srs())
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

    if (m_delta)
    {
        const uint64_t size(m_schema->find("X").size());
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

        const Bounds ex(mn, mx);

        if (!ex.contains(*m_boundsScaledCubic))
        {
            std::cout <<
                "Maximal extents: " << ex << "\n" <<
                "Scaled bounds:   " << *m_boundsScaledCubic << std::endl;
            throw std::runtime_error(
                    "Bounds are too large for the selected scale");
        }
    }

    if (m_dataIo->type() == "laszip" && !m_delta)
    {
        throw std::runtime_error("Laszip output needs scaling.");
    }
}

Metadata::Metadata(const arbiter::Endpoint& ep, const Config& config)
    : Metadata(
            entwine::merge(
                config.json(),
                entwine::merge(
                    parse(ep.get("entwine" +
                            config.postfix() + ".json")),
                    parse(ep.get("entwine-build" +
                            config.postfix() + ".json")))),
            true)
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
    json["ticks"] = (Json::UInt64)m_ticks;
    json["numPoints"] = (Json::UInt64)m_files->totalPoints();

    if (m_srs.size()) json["srs"] = m_srs;
    if (m_reprojection) json["reprojection"] = m_reprojection->toJson();

    if (m_delta) json = entwine::merge(json, m_delta->toJson());

    if (m_transformation)
    {
        for (const double v : *m_transformation)
        {
            json["transformation"].append(v);
        }
    }

    json["dataType"] = m_dataIo->type();
    json["hierarchyType"] = "json"; // TODO.
    if (m_hierarchyStep) json["hierarchyStep"] = (Json::UInt64)m_hierarchyStep;

    return json;
}

Json::Value Metadata::toBuildParamsJson() const
{
    Json::Value json;

    json["version"] = m_version->toString();
    json["trustHeaders"] = m_trustHeaders;
    json["overflowDepth"] = (Json::UInt64)m_overflowDepth;
    json["overflowThreshold"] = (Json::UInt64)m_overflowThreshold;
    if (m_subset) json["subset"] = m_subset->toJson();

    return json;
}

void Metadata::save(const arbiter::Endpoint& endpoint) const
{
    {
        const auto json(toJson());
        const std::string f("entwine" + postfix() + ".json");
        ensurePut(endpoint, f, json.toStyledString());
    }

    {
        const auto json(toBuildParamsJson());
        const std::string f("entwine-build" + postfix() + ".json");
        ensurePut(endpoint, f, json.toStyledString());
    }

    m_files->save(endpoint, postfix());
}

void Metadata::merge(const Metadata& other)
{
    if (m_srs.empty()) m_srs = other.srs();
    m_files->merge(other.files());
}

void Metadata::makeWhole()
{
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

Bounds Metadata::makeNativeConformingBounds(const Bounds& b) const
{
    Point pmin(b.min());
    Point pmax(b.max());

    pmin = pmin.apply([](double d)
    {
        if (static_cast<double>(static_cast<int64_t>(d)) == d) return d - 1.0;
        else return std::floor(d);
    });

    pmax = pmax.apply([](double d)
    {
        if (static_cast<double>(static_cast<int64_t>(d)) == d) return d + 1.0;
        else return std::ceil(d);
    });

    return Bounds(pmin, pmax);
}

Bounds Metadata::makeNativeCube(const Bounds& b, const Delta* d) const
{
    const Offset offset(
            d ?
                d->offset() :
                b.mid().apply([](double d) { return std::round(d); }));

    const double maxDist(std::max(std::max(b.width(), b.depth()), b.height()));
    double r(maxDist / 2.0);

    if (static_cast<double>(static_cast<uint64_t>(r)) == r) r += 1.0;
    else r = std::ceil(r);

    while (!Bounds(offset - r, offset + r).contains(b)) r += 1.0;

    return Bounds(offset - r, offset + r);
}

} // namespace entwine


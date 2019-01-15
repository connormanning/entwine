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
                    Bounds(jsoncppToMjson(config["boundsConforming"])) :
                    makeConformingBounds(Bounds(jsoncppToMjson(config["bounds"])))))
    , m_boundsCubic(makeUnique<Bounds>(
                exists ?
                    Bounds(jsoncppToMjson(config["bounds"])) :
                    makeCube(*m_boundsConforming)))
    , m_files(makeUnique<Files>(config.input()))
    , m_dataIo(DataIo::create(*this, config.dataType()))
    , m_reprojection(Reprojection::create(jsoncppToMjson(config["reprojection"])))
    , m_eptVersion(exists ?
            makeUnique<Version>(config["version"].asString()) :
            makeUnique<Version>(currentEptVersion()))
    , m_srs(makeUnique<Srs>(config.srs()))
    , m_subset(Subset::create(*this, jsoncppToMjson(config["subset"])))
    , m_trustHeaders(config.trustHeaders())
    , m_span(config.span())
    , m_startDepth(std::log2(m_span))
    , m_sharedDepth(m_subset ? m_subset->splits() : 0)
    , m_overflowDepth(std::max(config.overflowDepth(), m_sharedDepth))
    , m_overflowThreshold(config.overflowThreshold())
{
    if (1ULL << m_startDepth != m_span)
    {
        throw std::runtime_error("Invalid voxel span");
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
                config.get(),
                entwine::merge(
                    parse(ep.get("ept-build" + config.postfix() + ".json")),
                    parse(ep.get("ept" + config.postfix() + ".json")))),
            true)
{
    Files files(Files::extract(ep, primary(), config.postfix()));
    files.append(m_files->list());
    m_files = makeUnique<Files>(files.list());
}

Metadata::~Metadata() { }

Json::Value Metadata::toJson() const
{
    return mjsonToJsoncpp(json {
        { "version", eptVersion().toString() },
        { "bounds", boundsCubic() },
        { "boundsConforming", boundsConforming() },
        { "schema", jsoncppToMjson(m_outSchema->toJson()) },
        { "span", m_span },
        { "points", m_files->totalInserts() },
        { "dataType", m_dataIo->type() },
        { "hierarchyType", "json" }, // TODO
        { "srs", *m_srs }
    });
}

Json::Value Metadata::toBuildParamsJson() const
{
    json j {
        { "version", currentEntwineVersion().toString() },
        { "trustHeaders", m_trustHeaders },
        { "overflowDepth", m_overflowDepth },
        { "overflowThreshold", m_overflowThreshold },
        { "software", "Entwine" }
    };
    if (m_subset) j["subset"] = *m_subset;
    if (m_reprojection) j["reprojection"] = *m_reprojection;

    return mjsonToJsoncpp(j);
}

void Metadata::save(const arbiter::Endpoint& ep, const Config& config) const
{
    {
        const auto json(toJson());
        const std::string f("ept" + postfix() + ".json");
        ensurePut(ep, f, toPreciseString(json));
    }

    {
        const auto json(toBuildParamsJson());
        const std::string f("ept-build" + postfix() + ".json");
        ensurePut(ep, f, toPreciseString(json));
    }

    const bool detailed(!m_merged && primary());
    m_files->save(ep, postfix(), config, detailed);
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

Bounds Metadata::makeConformingBounds(Bounds b) const
{
    Point pmin(b.min());
    Point pmax(b.max());

    if (auto so = m_outSchema->scaleOffset())
    {
        pmin = so->clip(pmin);
        pmax = so->clip(pmax);
    }

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


/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/registry.hpp>

#include <pdal/PointView.hpp>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/self-chunk.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Registry::Registry(
        const Metadata& metadata,
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        PointPool& pointPool,
        Pool& threadPool,
        const bool exists)
    : m_metadata(metadata)
    , m_out(out)
    , m_tmp(tmp)
    , m_threadPool(threadPool)
    , m_root(ChunkKey(metadata), out, tmp, pointPool, m_hierarchy)
{
    std::size_t chunksAcross(1);
    std::size_t pointsAcross(1);
    const std::size_t maxDepth(64);
    m_slices.reserve(maxDepth);

    const auto& s(m_metadata.structure());

    for (std::size_t d(0); d < maxDepth; ++d)
    {
        m_slices.emplace_back(
                m_metadata,
                m_out,
                m_tmp,
                pointPool,
                d,
                chunksAcross,
                pointsAcross);

        if (d >= s.body() && d < s.tail()) chunksAcross *= 2;
        else pointsAcross *= 2;
    }

    if (exists)
    {
        const auto h(parse(out.get(
                        "entwine-hierarchy" + m_metadata.postfix() + ".json")));

        for (const auto key : h.getMemberNames())
        {
            const Dxyz dxyz(key);
            const uint64_t np(h[key].asUInt64());
            m_slices.at(dxyz.d).setNp(dxyz.p, np);
        }
    }
}

void Registry::save(const arbiter::Endpoint& endpoint) const
{
    const std::string f("entwine-hierarchy" + m_metadata.postfix() + ".json");
    io::ensurePut(endpoint, f, m_hierarchy.toJson().toStyledString());
}

void Registry::merge(const Registry& other, NewClipper& clipper)
{
    const auto& s(m_metadata.structure());
    Json::Value h;
    other.hierarchy(h, s.body(), Xyz());

    NewClimber climber(m_metadata);

    for (const std::string key : h.getMemberNames())
    {
        const Dxyz dxyz(key);

        if (dxyz.d < s.shared())
        {
            auto cells(other.m_slices.at(dxyz.d).read(dxyz.p));

            while (!cells.empty())
            {
                auto cell(cells.popOne());
                climber.init(cell->point(), dxyz.d);
                if (!m_slices.at(dxyz.d).insert(cell, climber, clipper).done())
                {
                    throw std::runtime_error(
                            "Invalid merge insert: " + dxyz.toString());
                }
            }
        }
        else
        {
            const uint64_t np(h[key].asUInt64());
            auto& slice(m_slices.at(dxyz.d));
            slice.setNp(dxyz.p, slice.np(dxyz.p) + np);
        }
    }
}

void Registry::hierarchy(
        Json::Value& h,
        uint64_t d,
        Xyz p,
        const uint64_t maxDepth) const
{
    h[p.toString(d)] = static_cast<Json::UInt64>(m_slices[d].np(p));

    ++d;
    if (maxDepth && d >= maxDepth) return;

    const auto& s(m_slices[d]);

    if (d <= m_metadata.structure().tail())
    {
        p.x <<= 1u;
        p.y <<= 1u;
        p.z <<= 1u;

        for (std::size_t a(0); a < 2; ++a)
        {
            for (std::size_t b(0); b < 2; ++b)
            {
                for (std::size_t c(0); c < 2; ++c)
                {
                    Xyz next(p.x + a, p.y + b, p.z + c);
                    if (s.np(next)) hierarchy(h, d, next);
                }
            }
        }
    }
    else if (s.np(p)) hierarchy(h, d, p);
}

} // namespace entwine


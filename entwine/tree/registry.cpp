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
#include <entwine/tree/clipper.hpp>
#include <entwine/tree/new-clipper.hpp>
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
        const bool exists)
    : m_metadata(metadata)
    , m_out(out)
    , m_tmp(tmp)
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
}

Registry::~Registry() { }

void Registry::save(const arbiter::Endpoint& endpoint) const
{
    Json::Value h;
    hierarchy(h, m_metadata.structure().body(), 0, 0);
    const std::string f("entwine-hierarchy" + m_metadata.postfix() + ".json");
    io::ensurePut(endpoint, f, h.toStyledString());
}

void Registry::hierarchy(
        Json::Value& h,
        uint64_t d,
        uint64_t x,
        uint64_t y) const
{
    h["n"] = static_cast<Json::UInt64>(m_slices[d].np(x, y));

    auto next([this, &h](uint64_t d, uint64_t x, uint64_t y)
    {
        const std::string key(
                d < 10 ? "0" : "" +
                std::to_string(d) + '/' +
                std::to_string(x) + '/' +
                std::to_string(y));
        hierarchy(h[key], d, x, y);
    });

    ++d;
    const auto& s(m_slices[d]);

    if (d <= m_metadata.structure().tail())
    {
        x <<= 1u;
        y <<= 1u;

        if (s.np(x, y)) next(d, x, y);
        if (s.np(x, y + 1)) next(d, x, y + 1);
        if (s.np(x + 1, y)) next(d, x + 1, y);
        if (s.np(x + 1, y + 1)) next(d, x + 1, y + 1);
    }
    else if (s.np(x, y)) next(d, x, y);
}

bool Registry::addPoint(
        Cell::PooledNode& cell,
        NewClimber& climber,
        NewClipper& clipper,
        const std::size_t maxDepth)
{
    Tube::Insertion attempt;

    while (true)
    {
        auto& slice(m_slices.at(climber.depth()));
        attempt = slice.insert(cell, climber, clipper);

        if (!attempt.done()) climber.step(cell->point());
        else return true;
    }
}

void Registry::clip(
        const uint64_t d,
        const uint64_t x,
        const uint64_t y,
        const uint64_t o)
{
    m_slices.at(d).clip(x, y, o);
}

} // namespace entwine


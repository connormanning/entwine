/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>
#include <limits>
#include <memory>

#include <json/json.h>

#include <entwine/types/bounds.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class NewQueryParams
{
public:
    NewQueryParams(
            std::size_t depth,
            const Json::Value& filter = Json::Value())
        : NewQueryParams(depth, depth ? depth + 1 : 0, filter)
    { }

    NewQueryParams(
            std::size_t depthBegin,
            std::size_t depthEnd,
            const Json::Value& filter = Json::Value())
        : NewQueryParams(Bounds::everything(), depthBegin, depthEnd, filter)
    { }

    NewQueryParams(
            const Bounds& bounds,
            std::size_t depth,
            const Json::Value& filter = Json::Value())
        : NewQueryParams(bounds, depth, depth ? depth + 1 : 0, filter)
    { }

    NewQueryParams(
            const Bounds& bounds,
            std::size_t depthBegin,
            std::size_t depthEnd,
            const Json::Value& filter = Json::Value())
        : NewQueryParams(bounds, Delta(), depthBegin, depthEnd, filter)
    { }

    NewQueryParams(
            const Bounds& bounds,
            const Delta& delta,
            std::size_t depth,
            const Json::Value& filter = Json::Value())
        : NewQueryParams(bounds, delta, depth, depth ? depth + 1 : 0, filter)
    { }

    NewQueryParams(
            const Bounds& bounds = Bounds::everything(),
            const Delta& delta = Delta(),
            std::size_t depthBegin = 0,
            std::size_t depthEnd = 0,
            const Json::Value& filter = Json::Value())
        : m_bounds(bounds)
        , m_delta(delta)
        , m_depthBegin(depthBegin)
        , m_depthEnd(depthEnd ? depthEnd : 64)
        , m_filter(filter)
    { }

    NewQueryParams(Json::Value q)
        : NewQueryParams(
            q.isMember("bounds") ?  Bounds(q["bounds"]) : Bounds::everything(),
            Delta(q),
            q.isMember("depth") ?
                q["depth"].asUInt64() : q["depthBegin"].asUInt64(),
            q.isMember("depth") ?
                q["depth"].asUInt64() + 1 : q["depthEnd"].asUInt64(),
            q["filter"])
    {
        if (q.isMember("depth"))
        {
            if (q.isMember("depthBegin") || q.isMember("depthEnd"))
            {
                std::cout << q << std::endl;
                throw std::runtime_error("Invalid depth specification");
            }
        }

        if (q.isMember("nativeBounds"))
        {
            if (q.isMember("bounds"))
            {
                std::cout << q << std::endl;
                throw std::runtime_error("Cannot specify multiple bounds");
            }

            m_nativeBounds = std::make_shared<Bounds>(q["nativeBounds"]);
        }
    }

    const Bounds& bounds() const { return m_bounds; }
    const Delta& delta() const { return m_delta; }
    std::size_t db() const { return m_depthBegin; }
    std::size_t de() const { return m_depthEnd; }
    const Json::Value& filter() const { return m_filter; }

    const Bounds* nativeBounds() const { return m_nativeBounds.get(); }

    NewQueryParams finalize(const Metadata& m) const
    {
        Delta d(localize(m, delta()));
        Bounds b(localize(m, bounds(), d));

        if (const Bounds* n = nativeBounds())
        {
            d = delta();
            b = localize(m, *n, m.delta()->inverse());
        }

        return NewQueryParams(b, d, db(), de(), filter());
    }

private:
    Delta localize(const Metadata& m, const Delta& out) const
    {
        const Delta in(m.delta());
        return Delta(out.scale() / in.scale(), out.offset() - in.offset());
    }

    Bounds localize(
            const Metadata& m,
            const Bounds& q,
            const Delta& local) const
    {
        const auto e(Bounds::everything());
        if (local.empty() || q == e) return q;

        const Bounds indexedBounds(m.boundsScaledCubic());

        const Point refCenter(
                Bounds(
                    Point::scale(
                        indexedBounds.min(),
                        indexedBounds.mid(),
                        local.scale(),
                        local.offset()),
                    Point::scale(
                        indexedBounds.max(),
                        indexedBounds.mid(),
                        local.scale(),
                        local.offset())).mid());

        const Bounds queryTransformed(
                Point::unscale(q.min(), Point(), local.scale(), -refCenter),
                Point::unscale(q.max(), Point(), local.scale(), -refCenter));

        Bounds queryCube(
                queryTransformed.min() + indexedBounds.mid(),
                queryTransformed.max() + indexedBounds.mid());

        // If the query bounds were 2d, make sure we maintain maximal extents.
        if (!q.is3d())
        {
            queryCube = Bounds(
                    Point(queryCube.min().x, queryCube.min().y, e.min().z),
                    Point(queryCube.max().x, queryCube.max().y, e.max().z));
        }

        queryCube.shrink(indexedBounds);

        return queryCube;
    }

    const Bounds m_bounds;
    const Delta m_delta;
    const std::size_t m_depthBegin;
    const std::size_t m_depthEnd;
    const Json::Value m_filter;

    std::shared_ptr<Bounds> m_nativeBounds;
};

} // namespace entwine


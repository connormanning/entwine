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
#include <memory>

#include <json/json.h>

#include <entwine/types/bounds.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class QueryParams
{
public:
    QueryParams(
            std::size_t depth,
            const Json::Value& filter = Json::Value())
        : QueryParams(depth, depth ? depth + 1 : 0, filter)
    { }

    QueryParams(
            std::size_t depthBegin,
            std::size_t depthEnd,
            const Json::Value& filter = Json::Value())
        : QueryParams(Bounds::everything(), depthBegin, depthEnd, filter)
    { }

    QueryParams(
            const Bounds& bounds,
            std::size_t depth,
            const Json::Value& filter = Json::Value())
        : QueryParams(bounds, depth, depth ? depth + 1 : 0, filter)
    { }

    QueryParams(
            const Bounds& bounds,
            std::size_t depthBegin,
            std::size_t depthEnd,
            const Json::Value& filter = Json::Value())
        : QueryParams(bounds, Delta(), depthBegin, depthEnd, filter)
    { }

    QueryParams(
            const Bounds& bounds,
            const Delta& delta,
            std::size_t depth,
            const Json::Value& filter = Json::Value())
        : QueryParams(bounds, delta, depth, depth ? depth + 1 : 0, filter)
    { }

    QueryParams(
            const Bounds& bounds = Bounds::everything(),
            const Delta& delta = Delta(),
            std::size_t depthBegin = 0,
            std::size_t depthEnd = 0,
            const Json::Value& filter = Json::Value())
        : m_bounds(bounds)
        , m_delta(delta)
        , m_depthBegin(depthBegin)
        , m_depthEnd(depthEnd)
        , m_filter(filter)
    { }

    QueryParams(Json::Value q)
        : QueryParams(
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

private:
    const Bounds m_bounds;
    const Delta m_delta;
    const std::size_t m_depthBegin;
    const std::size_t m_depthEnd;
    const Json::Value m_filter;

    std::shared_ptr<Bounds> m_nativeBounds;
};

} // namespace entwine


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

#include <entwine/types/bounds.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class QueryParams
{
public:
    QueryParams(
            std::size_t depth,
            const json& filter = json::object())
        : QueryParams(depth, depth ? depth + 1 : 0, filter)
    { }

    QueryParams(
            std::size_t depthBegin,
            std::size_t depthEnd,
            const json& filter = json::object())
        : QueryParams(Bounds::everything(), depthBegin, depthEnd, filter)
    { }

    QueryParams(
            const Bounds& bounds,
            std::size_t depth,
            const json& filter = json::object())
        : QueryParams(bounds, depth, depth ? depth + 1 : 0, filter)
    { }

    QueryParams(
            const Bounds& bounds = Bounds::everything(),
            std::size_t depthBegin = 0,
            std::size_t depthEnd = 0,
            const json& filter = json::object())
        : m_bounds(bounds)
        , m_depthBegin(depthBegin)
        , m_depthEnd(depthEnd ? depthEnd : 64)
        , m_filter(filter)
    { }

    QueryParams(json q)
        : QueryParams(
            q.count("bounds") ?
                Bounds(q.at("bounds")) : Bounds::everything(),
            q.count("depth") ?
                q.at("depth").get<uint64_t>() : q.value("depthBegin", 0),
            q.count("depth") ?
                q.at("depth").get<uint64_t>() + 1 : q.value("depthEnd", 0),
            q.value("filter", json()))
    {
        if (q.count("depth"))
        {
            if (q.count("depthBegin") || q.count("depthEnd"))
            {
                throw std::runtime_error(
                        "Invalid depth specification: " + q.dump(2));
            }
        }
    }

    const Bounds& bounds() const { return m_bounds; }
    std::size_t db() const { return m_depthBegin; }
    std::size_t de() const { return m_depthEnd; }
    const json& filter() const { return m_filter; }

private:
    const Bounds m_bounds;
    const std::size_t m_depthBegin = 0;
    const std::size_t m_depthEnd = 0;
    const json m_filter;
};

} // namespace entwine


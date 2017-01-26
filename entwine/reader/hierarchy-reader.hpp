/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>
#include <deque>
#include <mutex>
#include <vector>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/hierarchy.hpp>

namespace entwine
{

class Cache;

class HierarchyReader : public Hierarchy
{
public:
    HierarchyReader(
            HierarchyCell::Pool& pool,
            const Metadata& metadata,
            const arbiter::Endpoint& top,
            Cache& cache)
        : Hierarchy(pool, metadata, top, nullptr, true, true)
        , m_cache(cache)
    { }

    Json::Value query(
            const Bounds& queryBounds,
            std::size_t depthBegin,
            std::size_t depthEnd);

    Json::Value queryVertical(
            const Bounds& queryBounds,
            std::size_t depthBegin,
            std::size_t depthEnd);

private:
    class Query
    {
    public:
        Query(
                const Bounds& bounds,
                std::size_t depthBegin,
                std::size_t depthEnd)
            : m_bounds(bounds)
            , m_depthBegin(depthBegin)
            , m_depthEnd(depthEnd)
        { }

        const Bounds& bounds() const { return m_bounds; }
        std::size_t depthBegin() const { return m_depthBegin; }
        std::size_t depthEnd() const { return m_depthEnd; }

    private:
        const Bounds m_bounds;
        const std::size_t m_depthBegin;
        const std::size_t m_depthEnd;
    };

    class Reservation
    {
    public:
        Reservation(Cache& cache, std::string path)
            : m_cache(cache)
            , m_path(path)
        { }

        ~Reservation();
        void insert(const Slot* slot);

    private:
        Cache& m_cache;
        const std::string m_path;
        Slots m_slots;
    };

    void traverse(
            Json::Value& json,
            Reservation& reservation,
            const Query& query,
            const PointState& pointState,
            std::deque<Dir>& lag);

    void accumulate(
            Json::Value& json,
            Reservation& reservation,
            const Query& query,
            const PointState& pointState,
            std::deque<Dir>& lag,
            uint64_t inc);

    void reduce(
            std::vector<std::size_t>& out,
            std::size_t depth,
            const Json::Value& in) const;

    void maybeReserve(
            Reservation& reservation,
            const PointState& pointState) const;

    Cache& m_cache;
    std::mutex m_mutex;
};

} // namespace entwine


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
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

#include <entwine/reader/query.hpp>
#include <entwine/tree/hierarchy.hpp>
#include <entwine/types/file-info.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/outer-scope.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/third/arbiter/arbiter.hpp>

namespace entwine
{

class InvalidQuery : public std::runtime_error
{
public:
    InvalidQuery()
        : std::runtime_error("Invalid query")
    { }

    InvalidQuery(std::string what)
        : std::runtime_error("Invalid query - " + what)
    { }
};

class BaseChunkReader;
class Bounds;
class Cache;
class Hierarchy;
class Schema;

class Reader
{
public:
    // Will throw if entwine's meta files cannot be fetched from this endpoint.
    Reader(const arbiter::Endpoint& endpoint, Cache& cache);
    Reader(std::string path, Cache& cache);
    ~Reader();

    // Data queries.
    template<typename... Args>
    std::vector<char> query(Args&&... args)
    {
        auto q(getQuery(std::forward<Args>(args)...));
        return q->run();
    }

    std::unique_ptr<Query> getQuery(
            std::size_t depth,
            const Point* scale = nullptr,
            const Point* offset = nullptr);

    std::unique_ptr<Query> getQuery(
            const Bounds& qbox,
            std::size_t depth,
            const Point* scale = nullptr,
            const Point* offset = nullptr);

    std::unique_ptr<Query> getQuery(
            std::size_t depthBegin,
            std::size_t depthEnd,
            const Point* scale = nullptr,
            const Point* offset = nullptr);

    std::unique_ptr<Query> getQuery(
            const Bounds& qbox,
            std::size_t depthBegin,
            std::size_t depthEnd,
            const Point* scale = nullptr,
            const Point* offset = nullptr);

    std::unique_ptr<Query> getQuery(
            const Schema& schema,
            const Json::Value& filter,
            std::size_t depthBegin,
            std::size_t depthEnd,
            const Point* scale = nullptr,
            const Point* offset = nullptr);

    std::unique_ptr<Query> getQuery(
            const Schema& schema,
            const Json::Value& filter,
            const Bounds& qbox,
            std::size_t depthBegin,
            std::size_t depthEnd,
            const Point* scale = nullptr,
            const Point* offset = nullptr);

    std::unique_ptr<Query> getQuery(const Json::Value& json);

    // Hierarchy query.
    Json::Value hierarchy(
            const Bounds& qbox,
            std::size_t depthBegin,
            std::size_t depthEnd,
            bool vertical = false,
            const Point* scale = nullptr,
            const Point* offset = nullptr);

    Json::Value hierarchy(const Json::Value& json);

    // File metadata queries.
    FileInfo files(Origin origin) const;
    FileInfoList files(const std::vector<Origin>& origins) const;

    FileInfo files(std::string search) const;
    FileInfoList files(const std::vector<std::string>& searches) const;

    FileInfoList files(
            const Bounds& bounds,
            const Point* scale = nullptr,
            const Point* offset = nullptr) const;

    FileInfo files(const char* search) const
    {
        return files(std::string(search));
    }

    // Miscellaneous.
    const Metadata& metadata() const { return m_metadata; }
    PointPool& pool() const { return m_pool; }
    std::string path() const { return m_endpoint.root(); }

    const BaseChunkReader* base() const { return m_base.get(); }
    const arbiter::Endpoint& endpoint() const { return m_endpoint; }
    bool exists(const QueryChunkState& state) const;

private:
    void init();

    Delta localizeDelta(const Point* scale, const Point* offset) const;
    Bounds localize(
            const Bounds& inBounds,
            const Delta& localDelta) const;

    std::unique_ptr<arbiter::Arbiter> m_ownedArbiter;
    arbiter::Endpoint m_endpoint;

    const Metadata m_metadata;
    mutable PointPool m_pool;
    Cache& m_cache;

    std::unique_ptr<HierarchyReader> m_hierarchy;
    std::unique_ptr<BaseChunkReader> m_base;

    // Outer vector is organized by depth.
    std::vector<std::vector<Id>> m_ids;

    Pool m_threadPool;
    bool m_ready = false;

    mutable std::mutex m_mutex;
    mutable std::map<Id, bool> m_pre;
};

} // namespace entwine


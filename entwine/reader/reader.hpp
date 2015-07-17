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

#include <condition_variable>
#include <cstddef>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

#include <entwine/reader/cache.hpp>
#include <entwine/reader/chunk-reader.hpp>

namespace arbiter
{
    class Arbiter;
    class Endpoint;
}

namespace entwine
{

class BBox;
class Climber;
class Manifest;
class Query;
class Reprojection;
class Schema;
class Stats;
class Structure;

class Reader
{
public:
    // Will throw if entwine's meta files cannot be fetched from this endpoint.
    Reader(
            arbiter::Endpoint& endpoint,
            arbiter::Arbiter& arbiter,
            std::shared_ptr<Cache> cache);
    ~Reader();

    std::unique_ptr<Query> query(
            const Schema& schema,
            std::size_t depthBegin,
            std::size_t depthEnd);

    std::unique_ptr<Query> query(
            const Schema& schema,
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    std::size_t numPoints() const;
    const Schema& schema() const { return *m_schema; }
    const BBox& bbox() const { return *m_bbox; }

private:
    FetchInfoSet traverse(
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd) const;

    void traverse(
            FetchInfoSet& toFetch,
            std::size_t& tries,
            const Climber& climber,
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd) const;

    std::unique_ptr<Query> runQuery(
            std::unique_ptr<Block> block,
            const Schema& schema,
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    void runQuery(
            Query& query,
            const Climber& climber,
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd) const;

    const char* getPointPos(std::size_t index, const ChunkMap& chunks) const;
    std::size_t getChunkId(std::size_t index, std::size_t depth) const;

    // Returns 0 if chunk doesn't exist.
    arbiter::Endpoint* getEndpoint(std::size_t chunkId) const;

    std::string m_path;

    std::unique_ptr<BBox> m_bbox;
    std::unique_ptr<Schema> m_schema;
    std::unique_ptr<Structure> m_structure;
    std::unique_ptr<Reprojection> m_reprojection;
    std::unique_ptr<Manifest> m_manifest;
    std::unique_ptr<Stats> m_stats;
    std::unique_ptr<ChunkReader> m_base;
    std::shared_ptr<Cache> m_cache;

    bool m_is3d;

    mutable std::mutex m_mutex;
    mutable std::set<std::size_t> m_missing;

    std::map<std::unique_ptr<arbiter::Endpoint>, std::set<std::size_t>> m_ids;
};

} // namespace entwine


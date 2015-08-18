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
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

#include <entwine/reader/cache.hpp>
#include <entwine/reader/chunk-reader.hpp>
#include <entwine/types/structure.hpp>

namespace arbiter
{
    class Arbiter;
    class Endpoint;
}

namespace entwine
{

class BBox;
class ChunkClimber;
class Climber;
class LinkingPointView;
class Manifest;
class Query;
class Reprojection;
class Schema;
class SinglePointTable;
class Stats;

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
    const BBox& bbox() const            { return *m_bbox; }
    const Schema& schema() const        { return *m_schema; }
    const Structure& structure() const  { return *m_structure; }

private:
    FetchInfoSet traverse(
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd) const;

    void traverse(
            FetchInfoSet& toFetch,
            const ChunkClimber& climber,
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
            std::size_t depthEnd,
            SinglePointTable& table,
            LinkingPointView& view,
            std::vector<int> traversal = std::vector<int>()) const;

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
    mutable std::set<Id> m_missing;

    std::map<std::unique_ptr<arbiter::Endpoint>, std::set<Id>> m_ids;
};

} // namespace entwine


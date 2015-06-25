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

#include <entwine/drivers/source.hpp>
#include <entwine/reader/cache.hpp>
#include <entwine/reader/chunk-reader.hpp>

namespace entwine
{

class Arbiter;
class BBox;
class Manifest;
class Query;
class Reprojection;
class Roller;
class Schema;
class Stats;
class Structure;

class QueryLimitExceeded : public std::runtime_error
{
public:
    QueryLimitExceeded() : std::runtime_error("Query size limit exceeded") { }
};

class Reader
{
public:
    // Will throw if entwine's meta files cannot be fetched from this source.
    Reader(Source source, Arbiter& arbiter, std::shared_ptr<Cache> cache);
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
            const Roller& roller,
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
            const Roller& roller,
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd) const;

    const char* getPointPos(std::size_t index, const ChunkMap& chunks) const;
    std::size_t getChunkId(std::size_t index, std::size_t depth) const;

    // Returns 0 if chunk doesn't exist.
    Source* getSource(std::size_t chunkId) const;

    std::string m_path;

    std::unique_ptr<BBox> m_bbox;
    std::unique_ptr<Schema> m_schema;
    std::unique_ptr<Structure> m_structure;
    std::unique_ptr<Reprojection> m_reprojection;
    std::unique_ptr<Manifest> m_manifest;
    std::unique_ptr<Stats> m_stats;
    std::unique_ptr<ChunkReader> m_base;
    std::shared_ptr<Cache> m_cache;

    std::map<std::unique_ptr<Source>, std::set<std::size_t>> m_ids;
};

} // namespace entwine


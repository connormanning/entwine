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
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/single-point-table.hpp>

namespace entwine
{

class Arbiter;
class BBox;
class ChunkReader;
class Driver;
class Manifest;
class Point;
class Reader;
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

typedef std::map<std::size_t, const ChunkReader*> ChunkMap;

class Query
{
public:
    Query(
            Reader& reader,
            const Schema& outSchema,
            ChunkMap chunkMap);

    // ~Query();

    void addPoint(const char* pos);

    Point unwrapPoint(const char* pos) const;
    std::size_t size() const;
    void getPointAt(std::size_t index, char* out) const;

    const ChunkMap& chunkMap() const { return m_chunkMap; }

private:
    Reader& m_reader;
    ChunkMap m_chunkMap;
    const Schema& m_outSchema;

    std::vector<const char*> m_points;

    mutable SinglePointTable m_table;
    mutable LinkingPointView m_view;
};

class Reader
{
public:
    // Will throw if entwine's meta files cannot be fetched from this source.
    Reader(
            Source source,
            std::size_t cacheSize,
            std::size_t queryLimit,
            std::shared_ptr<Arbiter> arbiter);

    ~Reader();

    Query query(
            const Schema& schema,
            std::size_t depthBegin,
            std::size_t depthEnd);

    Query query(
            const Schema& schema,
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    std::size_t numPoints() const;
    const Schema& schema() const { return *m_schema; }
    const BBox& bbox() const { return *m_bbox; }

private:
    std::set<std::size_t> traverse(
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd) const;

    void traverse(
            std::set<std::size_t>& toFetch,
            const Roller& roller,
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd) const;

    Query runQuery(
            const ChunkMap& chunkMap,
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

    const char* getPointPos(std::size_t index, const ChunkMap& chunkMap) const;
    std::size_t getChunkId(std::size_t index, std::size_t depth) const;

    // Returns 0 if chunk doesn't exist.
    Source* getSource(std::size_t chunkId) const;

    ChunkMap warm(const std::set<std::size_t>& toFetch);

    // This is the only place in which we mutate our state.
    const ChunkReader* fetch(std::size_t chunkId);

    std::unique_ptr<BBox> m_bbox;
    std::unique_ptr<Schema> m_schema;
    std::unique_ptr<Structure> m_structure;
    std::unique_ptr<Reprojection> m_reprojection;
    std::unique_ptr<Manifest> m_manifest;
    std::unique_ptr<Stats> m_stats;
    std::unique_ptr<SinglePointTable> m_table;
    std::unique_ptr<LinkingPointView> m_view;

    std::map<std::unique_ptr<Source>, std::set<std::size_t>> m_ids;
    std::shared_ptr<Arbiter> m_arbiter;

    const std::size_t m_cacheSize;
    const std::size_t m_queryLimit;
    std::mutex m_mutex;
    std::condition_variable m_cv;
    std::unique_ptr<ChunkReader> m_base;
    std::map<std::size_t, std::unique_ptr<ChunkReader>> m_chunks;

    // Currently being fetched.
    std::set<std::size_t> m_outstanding;

    // Ordered by last-access time.
    std::list<std::size_t> m_accessList;
    std::map<std::size_t, std::list<std::size_t>::iterator> m_accessMap;
};

} // namespace entwine


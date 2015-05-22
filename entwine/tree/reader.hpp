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

namespace entwine
{

class BBox;
class ChunkReader;
class Driver;
class Manifest;
class Point;
class Pool;
class Roller;
class Reprojection;
class Schema;
class Stats;
class Structure;

class Reader
{
public:
    // Will throw if entwine's meta files cannot be fetched from this source.
    Reader(Source source, std::size_t cacheSize, std::size_t queryLimit);
    ~Reader();

    // Query calls may throw if a cache overrun is detected.
    std::vector<std::size_t> query(
            std::size_t depthBegin,
            std::size_t depthEnd);

    std::vector<std::size_t> query(
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    // This should only be called on indices that have previously been returned
    // from a call to Reader::query(), which means that a point does exist at
    // this index in the tree and that the source data necessary to retrieve
    // this point has been fetched successfully.
    //
    // This method may throw in at least two cases:
    //      1) There is no point at this index.  A user should only call this
    //         function with an index previously returned by a call to query().
    //      2) There may be a valid point at this index, but this Reader is
    //         experiencing cache overrun.  Clients should limit their query
    //         rate.
    std::vector<char> getPointData(std::size_t index, const Schema& schema);

    std::size_t numPoints() const;
    const Schema& schema() const { return *m_schema; }
    const BBox& bbox() const { return *m_bbox; }

private:
    void traverse(
            std::set<std::size_t>& toFetch,
            const Roller& roller,
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    void warm(const std::set<std::size_t>& toFetch);

    void query(
            const Roller& roller,
            std::vector<std::size_t>& results,
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    // These queries require the chunk for this index to be pre-fetched.
    Point getPoint(std::size_t index);      // Caller must NOT lock.
    char* getPointData(std::size_t index);  // Caller must lock.

    std::size_t maxId() const;
    std::size_t getChunkId(std::size_t index) const;

    void fetch(std::size_t chunkId);

    std::unique_ptr<BBox> m_bbox;
    std::unique_ptr<Schema> m_schema;
    std::unique_ptr<Structure> m_structure;
    std::unique_ptr<Reprojection> m_reprojection;
    std::unique_ptr<Manifest> m_manifest;
    std::unique_ptr<Stats> m_stats;

    std::set<std::size_t> m_ids;

    Source m_source;

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


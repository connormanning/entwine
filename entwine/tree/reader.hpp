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
#include <set>
#include <vector>

#include <entwine/http/s3.hpp>

namespace entwine
{

class BBox;
class Point;
class Pool;
class Roller;
class Schema;

class Reader
{
public:
    Reader(const S3Info& s3Info, std::size_t cacheSize);

    std::vector<std::size_t> query(
            std::size_t depthBegin,
            std::size_t depthEnd);

    std::vector<std::size_t> query(
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    // Returns an empty vector if there is no point at this index.
    std::vector<char> getPointData(std::size_t index, const Schema& schema);

    std::size_t numPoints() const { return m_numPoints; }
    const Schema& schema() const { return *m_schema; }

private:
    void warm(
            const Roller& roller,
            Pool& pool,
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    void query(
            const Roller& roller,
            std::vector<std::size_t>& results,
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    // These queries require the chunk for this index to be pre-fetched.
    bool hasPoint(std::size_t index);
    Point getPoint(std::size_t index);
    char* getPointData(std::size_t index);  // Returns 0 if no point here.

    std::size_t maxId() const;
    std::size_t getChunkId(std::size_t index) const;

    void fetch(std::size_t chunkId);

    std::size_t m_firstChunk;
    std::size_t m_chunkPoints;
    std::set<std::size_t> m_ids;
    std::unique_ptr<BBox> m_bbox;
    std::unique_ptr<Schema> m_schema;

    std::size_t m_dimensions;
    std::size_t m_numPoints;
    std::size_t m_numTossed;

    std::vector<std::string> m_originList;

    std::unique_ptr<S3> m_s3;

    const std::size_t m_cacheSize;
    std::mutex m_mutex;
    std::condition_variable m_cv;
    std::unique_ptr<std::vector<char>> m_base;
    std::map<std::size_t, std::unique_ptr<std::vector<char>>> m_chunks;

    // Currently being fetched.
    std::set<std::size_t> m_outstanding;

    // Ordered by last-access time.
    std::list<std::size_t> m_accessList;
    std::map<std::size_t, std::list<std::size_t>::iterator> m_accessMap;
};

} // namespace entwine


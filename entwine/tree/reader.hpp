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
    ~Reader();

    std::vector<std::size_t> query(
            std::size_t depthBegin,
            std::size_t depthEnd);

    std::vector<std::size_t> query(
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    std::vector<char> getPointData(std::size_t index, const Schema& schema);

    std::size_t numPoints() const { return m_numPoints; }
    const Schema& schema() const { return *m_schema; }

private:
    void query(
            const Roller& roller,
            std::vector<std::size_t>& results,
            std::size_t depthBegin,
            std::size_t depthEnd);

    void query(
            const Roller& roller,
            std::vector<std::size_t>& results,
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    bool hasPoint(std::size_t index);
    Point getPoint(std::size_t index);
    std::vector<char> getPointData(std::size_t index);

    std::size_t m_firstChunk;
    std::size_t m_chunkPoints;
    std::set<std::size_t> m_ids;
    std::unique_ptr<BBox> m_bbox;
    std::unique_ptr<Schema> m_schema;

    std::size_t m_dimensions;
    std::size_t m_numPoints;
    std::size_t m_numTossed;

    std::vector<std::string> m_originList;

    std::unique_ptr<Pool> m_pool;
    std::unique_ptr<S3> m_s3;

    const std::size_t m_cacheSize;
    std::mutex m_mutex;
    std::condition_variable m_cv;
    std::unique_ptr<std::vector<char>> m_base;
    std::map<std::size_t, std::vector<char>*> m_chunks;
    std::set<std::size_t> m_outstanding;

    std::list<std::size_t> m_accessList;
    std::map<std::size_t, std::list<std::size_t>::iterator> m_accessMap;
};

} // namespace entwine


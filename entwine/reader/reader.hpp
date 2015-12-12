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

class BBox;
class Cache;
class Climber;
class BaseChunk;
class Manifest;
class Query;
class Reprojection;
class Schema;
class SinglePointTable;

class Reader
{
public:
    // Will throw if entwine's meta files cannot be fetched from this endpoint.
    Reader(
            const arbiter::Endpoint& endpoint,
            const arbiter::Arbiter& arbiter,
            Cache& cache);
    ~Reader();

    std::unique_ptr<Query> query(
            const Schema& schema,
            std::size_t depthBegin,
            std::size_t depthEnd,
            bool normalize);

    std::unique_ptr<Query> query(
            const Schema& schema,
            const BBox& qbox,
            std::size_t depthBegin,
            std::size_t depthEnd,
            bool normalize);

    std::size_t numPoints() const;
    const BBox& bbox() const            { return *m_bbox; }
    const Schema& schema() const        { return *m_schema; }
    const Structure& structure() const  { return *m_structure; }
    const std::string& srs() const      { return m_srs; }
    std::string path() const            { return m_endpoint.root(); }

    const BaseChunk* base() const { return m_base.get(); }
    const arbiter::Endpoint& endpoint() const { return m_endpoint; }
    bool exists(const Id& chunkId) const { return m_ids.count(chunkId); }

private:
    arbiter::Endpoint m_endpoint;

    std::unique_ptr<BBox> m_bbox;
    std::unique_ptr<Schema> m_schema;
    std::unique_ptr<Structure> m_structure;
    std::unique_ptr<Reprojection> m_reprojection;
    std::unique_ptr<Manifest> m_manifest;
    std::unique_ptr<BaseChunk> m_base;
    std::unique_ptr<Pools> m_pointPool;

    std::size_t m_numPoints;

    Cache& m_cache;
    std::string m_srs;
    std::set<Id> m_ids;
};

} // namespace entwine


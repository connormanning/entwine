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

class BaseChunk;
class BBox;
class Builder;
class Cache;
class Climber;
class Hierarchy;
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
            double scale = 0.0,
            Point offset = Point());

    std::unique_ptr<Query> query(
            const Schema& schema,
            const BBox& qbox,
            std::size_t depthBegin,
            std::size_t depthEnd,
            double scale = 0.0,
            Point offset = Point());

    Json::Value hierarchy(
            const BBox& qbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    std::size_t numPoints() const;
    const BBox& bbox() const;
    const Schema& schema() const;
    const Structure& structure() const;
    const std::string& srs() const;
    std::string path() const;

    const BaseChunk* base() const;
    const arbiter::Endpoint& endpoint() const;
    bool exists(const Id& id) const { return m_ids.count(id); }

    struct BoxInfo
    {
        BoxInfo() : keys(), numPoints(0) { }
        BoxInfo(std::vector<std::string> keys) : keys(keys), numPoints(0) { }

        std::vector<std::string> keys;
        std::size_t numPoints;
    };

    typedef std::map<BBox, BoxInfo> BoxMap;

private:
    arbiter::Endpoint m_endpoint;

    std::unique_ptr<Builder> m_builder;
    std::unique_ptr<BaseChunk> m_base;

    Cache& m_cache;
    std::set<Id> m_ids;
};

} // namespace entwine


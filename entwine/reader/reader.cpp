/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/reader/reader.hpp>

#include <numeric>

#include <entwine/reader/cache.hpp>
#include <entwine/reader/chunk-reader.hpp>
#include <entwine/reader/query.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/hierarchy.hpp>
#include <entwine/tree/manifest.hpp>
#include <entwine/tree/registry.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/compression.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    void checkQuery(std::size_t depthBegin, std::size_t depthEnd)
    {
        if (depthBegin >= depthEnd)
        {
            throw InvalidQuery("Invalid depths");
        }
    }

    HierarchyCell::Pool hierarchyPool(4096);
}

Reader::Reader(const arbiter::Endpoint& endpoint, Cache& cache)
    : m_endpoint(endpoint)
    , m_metadata(makeUnique<Metadata>(m_endpoint))
    , m_hierarchy(
            makeUnique<Hierarchy>(
                hierarchyPool,
                *m_metadata,
                endpoint,
                nullptr,
                true))
    , m_base()
    , m_cache(cache)
    , m_ids()
{
    const Structure& structure(m_metadata->structure());

    if (structure.hasBase())
    {
        auto compressed(
                makeUnique<std::vector<char>>(
                    endpoint.getBinary(structure.baseIndexBegin().str())));

        m_base = makeUnique<BaseChunkReader>(
                *m_metadata,
                BaseChunk::makeCelled(m_metadata->schema()),
                structure.baseIndexBegin(),
                std::move(compressed));
    }

    if (structure.hasCold())
    {
        auto ids(extractIds(m_endpoint.get("entwine-ids")));
        m_ids.insert(ids.begin(), ids.end());
        std::cout << "Found " << m_ids.size() << " chunks" << std::endl;
    }
}

Reader::~Reader()
{ }

Json::Value Reader::hierarchy(
        const Bounds& queryBounds,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const bool vertical)
{
    checkQuery(depthBegin, depthEnd);
    Hierarchy::QueryResults results(
            vertical ?
                m_hierarchy->queryVertical(queryBounds, depthBegin, depthEnd) :
                m_hierarchy->query(queryBounds, depthBegin, depthEnd));

    m_cache.markHierarchy(m_endpoint.prefixedRoot(), results.touched);

    return results.json;
}

std::unique_ptr<Query> Reader::query(
        const Schema& schema,
        const Json::Value& filter,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const double scale,
        const Point offset)
{
    return query(schema, filter, bounds(), depthBegin, depthEnd, scale, offset);
}

std::unique_ptr<Query> Reader::query(
        const Schema& schema,
        const Json::Value& filter,
        const Bounds& queryBounds,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const double scale,
        const Point offset)
{
    checkQuery(depthBegin, depthEnd);

    Bounds queryCube(queryBounds);

    if (!queryBounds.is3d())
    {
        // Make sure the query is 3D.
        queryCube = Bounds(
                Point(
                    queryBounds.min().x,
                    queryBounds.min().y,
                    bounds().min().z),
                Point(
                    queryBounds.max().x,
                    queryBounds.max().y,
                    bounds().max().z));
    }

    return std::unique_ptr<Query>(
            new Query(
                *this,
                schema,
                filter,
                m_cache,
                queryCube,
                depthBegin,
                depthEnd,
                scale,
                offset));
}

const Bounds& Reader::bounds() const { return m_metadata->bounds(); }

} // namespace entwine


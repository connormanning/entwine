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
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/hierarchy.hpp>
#include <entwine/tree/registry.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/manifest.hpp>
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

    arbiter::Arbiter defaultArbiter;
}

Reader::Reader(const std::string path, Cache& cache)
    : Reader(defaultArbiter.getEndpoint(path), cache)
{ }

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
        // std::cout << "Found " << m_ids.size() << " chunks" << std::endl;
    }
}

Reader::~Reader()
{ }

Json::Value Reader::hierarchy(
        const Bounds& inBounds,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const bool vertical,
        const Point* scale,
        const Point* offset)
{
    checkQuery(depthBegin, depthEnd);

    const Delta indexDelta(m_metadata->delta());
    const Delta queryDelta(scale, offset);
    const Delta localDelta(
            queryDelta.scale() / indexDelta.scale(),
            queryDelta.offset() - indexDelta.offset());

    const Bounds queryBounds(localize(inBounds, localDelta));

    Hierarchy::QueryResults results(
            vertical ?
                m_hierarchy->queryVertical(queryBounds, depthBegin, depthEnd) :
                m_hierarchy->query(queryBounds, depthBegin, depthEnd));

    m_cache.markHierarchy(m_endpoint.prefixedRoot(), results.touched);

    return results.json;
}

std::unique_ptr<Query> Reader::getQuery(
        std::size_t depth,
        const Point* scale,
        const Point* offset)
{
    return getQuery(depth, depth + 1, scale, offset);
}

std::unique_ptr<Query> Reader::getQuery(
        const Bounds& qbox,
        std::size_t depth,
        const Point* scale,
        const Point* offset)
{
    return getQuery(qbox, depth, depth + 1, scale, offset);
}

std::unique_ptr<Query> Reader::getQuery(
        std::size_t depthBegin,
        std::size_t depthEnd,
        const Point* scale,
        const Point* offset)
{
    return getQuery(
            m_metadata->schema(),
            Json::Value(),
            depthBegin,
            depthEnd,
            scale,
            offset);
}

std::unique_ptr<Query> Reader::getQuery(
        const Bounds& qbox,
        std::size_t depthBegin,
        std::size_t depthEnd,
        const Point* scale,
        const Point* offset)
{
    return getQuery(
            m_metadata->schema(),
            Json::Value(),
            qbox,
            depthBegin,
            depthEnd,
            scale,
            offset);
}

std::unique_ptr<Query> Reader::getQuery(
        const Schema& schema,
        const Json::Value& filter,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const Point* scale,
        const Point* offset)
{
    return getQuery(
            schema,
            filter,
            Bounds::everything(),
            depthBegin,
            depthEnd,
            scale,
            offset);
}

// All other getQuery operations fall through to this one.
std::unique_ptr<Query> Reader::getQuery(
        const Schema& schema,
        const Json::Value& filter,
        const Bounds& queryBounds,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const Point* scale,
        const Point* offset)
{
    checkQuery(depthBegin, depthEnd);

    Bounds queryCube(queryBounds);

    if (!queryCube.is3d())
    {
        queryCube = Bounds(
                Point(
                    queryCube.min().x,
                    queryCube.min().y,
                    std::numeric_limits<double>::lowest()),
                Point(
                    queryCube.max().x,
                    queryCube.max().y,
                    std::numeric_limits<double>::max()));
    }

    const Delta indexDelta(m_metadata->delta());
    const Delta queryDelta(scale, offset);
    const Delta localDelta(
            queryDelta.scale() / indexDelta.scale(),
            queryDelta.offset() - indexDelta.offset());

    return makeUnique<Query>(
            *this,
            schema,
            filter,
            m_cache,
            localize(queryCube, localDelta),
            depthBegin,
            depthEnd,
            localDelta.exists() ? &localDelta.scale() : nullptr,
            localDelta.exists() ? &localDelta.offset() : nullptr);
}

Bounds Reader::localize(
        const Bounds& queryBounds,
        const Delta& localDelta) const
{
    if (localDelta.empty() || queryBounds == Bounds::everything())
    {
        return queryBounds;
    }

    const Bounds indexedBounds(m_metadata->bounds());

    const Point queryReferenceCenter(
            Bounds(
                Point::scale(
                    indexedBounds.min(),
                    indexedBounds.mid(),
                    localDelta.scale(),
                    localDelta.offset()),
                Point::scale(
                    indexedBounds.max(),
                    indexedBounds.mid(),
                    localDelta.scale(),
                    localDelta.offset())).mid());

    const Bounds queryTransformed(
            Point::unscale(
                queryBounds.min(),
                Point(),
                localDelta.scale(),
                -queryReferenceCenter),
            Point::unscale(
                queryBounds.max(),
                Point(),
                localDelta.scale(),
                -queryReferenceCenter));

    Bounds queryCube(
            queryTransformed.min() + indexedBounds.mid(),
            queryTransformed.max() + indexedBounds.mid());

    // If the query bounds were 2d, make sure we maintain maximal extents.
    if (
            queryBounds.min().z == Bounds::everything().min().z &&
            queryBounds.max().z == Bounds::everything().max().z)
    {
        queryCube = Bounds(
                Point(
                    queryCube.min().x,
                    queryCube.min().y,
                    Bounds::everything().min().z),
                Point(
                    queryCube.max().x,
                    queryCube.max().y,
                    Bounds::everything().max().z));
    }

    return queryCube;
}

} // namespace entwine


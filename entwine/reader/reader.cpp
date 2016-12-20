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

    Bounds queryBounds(localize(inBounds, scale, offset));

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
    checkQuery(depthBegin, depthEnd);
    return getQuery(
            schema,
            filter,
            Bounds::everything(),
            depthBegin,
            depthEnd,
            scale,
            offset);
}

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

    return makeUnique<Query>(
            *this,
            schema,
            filter,
            m_cache,
            localize(queryCube, scale, offset),
            depthBegin,
            depthEnd,
            scale,
            offset);
}

const Bounds& Reader::bounds() const { return m_metadata->bounds(); }

Bounds Reader::localize(
        const Bounds& queryBounds,
        const Scale* scale,
        const Offset* offset) const
{
    const auto delta(Delta::maybeCreate(scale, offset));
    if (!delta || queryBounds == Bounds::everything()) return queryBounds;

    const Bounds& indexedBounds(m_metadata->bounds());
    const Point queryReferenceCenter(
            Bounds(
                Point::scale(
                    indexedBounds.min(),
                    indexedBounds.mid(),
                    delta->scale(),
                    delta->offset()),
                Point::scale(
                    indexedBounds.max(),
                    indexedBounds.mid(),
                    delta->scale(),
                    delta->offset())).mid());

    const Bounds queryTransformed(
            Point::unscale(
                queryBounds.min(),
                Point(),
                delta->scale(),
                -queryReferenceCenter),
            Point::unscale(
                queryBounds.max(),
                Point(),
                delta->scale(),
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


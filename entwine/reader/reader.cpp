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

#include <algorithm>
#include <numeric>

#include <entwine/reader/cache.hpp>
#include <entwine/reader/chunk-reader.hpp>
#include <entwine/reader/hierarchy-reader.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/registry.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/manifest.hpp>
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

    Bounds ensure3d(const Bounds& bounds)
    {
        if (bounds.is3d())
        {
            return bounds;
        }
        else
        {
            return Bounds(
                    Point(
                        bounds.min().x,
                        bounds.min().y,
                        std::numeric_limits<double>::lowest()),
                    Point(
                        bounds.max().x,
                        bounds.max().y,
                        std::numeric_limits<double>::max()));
        }
    }

    HierarchyCell::Pool hierarchyPool(4096);
}

Reader::Reader(const std::string path, Cache& cache)
    : m_ownedArbiter(makeUnique<arbiter::Arbiter>())
    , m_endpoint(m_ownedArbiter->getEndpoint(path))
    , m_metadata(m_endpoint)
    , m_pool(m_metadata.schema(), m_metadata.delta())
    , m_cache(cache)
    , m_hierarchy(
            makeUnique<HierarchyReader>(
                hierarchyPool,
                m_metadata,
                m_endpoint,
                m_cache))
    , m_threadPool(2)
{
    init();
}

Reader::Reader(const arbiter::Endpoint& endpoint, Cache& cache)
    : m_endpoint(endpoint)
    , m_metadata(m_endpoint)
    , m_pool(m_metadata.schema(), m_metadata.delta())
    , m_cache(cache)
    , m_hierarchy(
            makeUnique<HierarchyReader>(
                hierarchyPool,
                m_metadata,
                m_endpoint,
                m_cache))
    , m_threadPool(2)
{
    init();
}

void Reader::init()
{
    const Structure& structure(m_metadata.structure());

    if (structure.hasBase())
    {
        if (m_metadata.slicedBase())
        {
            m_base = makeUnique<SlicedBaseChunkReader>(
                    m_metadata,
                    m_pool,
                    m_endpoint);
        }
        else
        {
            m_base = makeUnique<CelledBaseChunkReader>(
                    m_metadata,
                    m_pool,
                    m_endpoint);
        }
    }

    if (structure.hasCold())
    {
        m_threadPool.add([&]()
        {
            const auto ids(extractIds(m_endpoint.get("entwine-ids")));
            if (ids.empty()) return;

            std::size_t depth(ChunkInfo::calcDepth(4, ids.front()));
            Id nextDepthIndex(ChunkInfo::calcLevelIndex(2, depth + 1));

            m_ids.resize(ChunkInfo::calcDepth(4, ids.back()) + 1);

            for (const auto& id : ids)
            {
                if (id >= nextDepthIndex)
                {
                    ++depth;
                    nextDepthIndex <<= structure.dimensions();
                    ++nextDepthIndex;
                }

                if (ChunkInfo::calcDepth(4, id) != depth)
                {
                    throw std::runtime_error("Invalid depth");
                }

                m_ids.at(depth).push_back(id);
            }

            std::cout << m_endpoint.prefixedRoot() << " ready" << std::endl;
            m_ready = true;
        });
    }
}

bool Reader::exists(const QueryChunkState& c) const
{
    if (m_ready)
    {
        if (c.depth() >= m_ids.size()) return false;
        const auto& slice(m_ids[c.depth()]);
        return std::binary_search(slice.begin(), slice.end(), c.chunkId());
    }
    else
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_pre.count(c.chunkId())) return m_pre[c.chunkId()];
        auto& val(m_pre[c.chunkId()]);
        val = false;

        const auto f(m_metadata.filename(c.chunkId()));
        if (const auto size = m_endpoint.tryGetSize(f)) val = *size;
        std::cout << m_endpoint.prefixedRoot() << f << ": " << val << std::endl;
        return val;
    }
}

Reader::~Reader()
{
    m_cache.release(*this);
}

Json::Value Reader::hierarchy(
        const Bounds& inBounds,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const bool vertical,
        const Point* scale,
        const Point* offset)
{
    checkQuery(depthBegin, depthEnd);

    const Bounds queryBounds(inBounds.undeltify(Delta(scale, offset)));
    return vertical ?
        m_hierarchy->queryVertical(queryBounds, depthBegin, depthEnd) :
        m_hierarchy->query(queryBounds, depthBegin, depthEnd);
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
            m_metadata.schema(),
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
            m_metadata.schema(),
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
        std::size_t depthEnd,
        const Point* scale,
        const Point* offset)
{
    if (!depthEnd) depthEnd = std::numeric_limits<uint32_t>::max();
    checkQuery(depthBegin, depthEnd);

    const Delta localDelta(localizeDelta(scale, offset));
    const Bounds localQueryCube(localize(ensure3d(queryBounds), localDelta));

    return makeUnique<Query>(
            *this,
            schema,
            filter,
            m_cache,
            localQueryCube,
            depthBegin,
            depthEnd,
            localDelta.exists() ? &localDelta.scale() : nullptr,
            localDelta.exists() ? &localDelta.offset() : nullptr);
}

FileInfo Reader::files(const Origin origin) const
{
    return m_metadata.manifest().get(origin);
}

FileInfoList Reader::files(const std::vector<Origin>& origins) const
{
    FileInfoList fileInfo;
    fileInfo.reserve(origins.size());
    for (const auto origin : origins) fileInfo.push_back(files(origin));
    return fileInfo;
}

FileInfo Reader::files(std::string search) const
{
    return files(m_metadata.manifest().find(search));
}

FileInfoList Reader::files(const std::vector<std::string>& searches) const
{
    FileInfoList fileInfo;
    fileInfo.reserve(searches.size());
    for (const auto& search : searches) fileInfo.push_back(files(search));
    return fileInfo;
}

FileInfoList Reader::files(
        const Bounds& queryBounds,
        const Point* scale,
        const Point* offset) const
{
    auto delta(Delta::maybeCreate(scale, offset));
    const Bounds absoluteBounds(
            delta ?
                queryBounds.unscale(delta->scale(), delta->offset()) :
                queryBounds);
    const Bounds absoluteCube(ensure3d(absoluteBounds));
    return files(m_metadata.manifest().find(absoluteCube));
}

Delta Reader::localizeDelta(const Point* scale, const Point* offset) const
{
    const Delta builtInDelta(m_metadata.delta());
    const Delta queryDelta(scale, offset);
    return Delta(
            queryDelta.scale() / builtInDelta.scale(),
            queryDelta.offset() - builtInDelta.offset());
}

Bounds Reader::localize(
        const Bounds& queryBounds,
        const Delta& localDelta) const
{
    if (localDelta.empty() || queryBounds == Bounds::everything())
    {
        return queryBounds;
    }

    const Bounds indexedBounds(m_metadata.boundsScaledCubic());

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


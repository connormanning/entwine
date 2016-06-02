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

#include <entwine/reader/cache.hpp>
#include <entwine/reader/query.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/hierarchy.hpp>
#include <entwine/tree/manifest.hpp>
#include <entwine/tree/registry.hpp>
#include <entwine/types/bbox.hpp>
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
}

Reader::Reader(
        const arbiter::Endpoint& endpoint,
        const arbiter::Arbiter& arbiter,
        Cache& cache)
    : m_endpoint(endpoint)
    , m_metadata(makeUnique<Metadata>(m_endpoint))
    , m_base()
    , m_cache(cache)
    , m_ids()
{
    /*
    if (m_metadata->structure().hasBase())
    {
        auto compressed(endpoint.getBinary(structure().baseIndexBegin().str()));
        const Tail tail(popTail(decompressed));

        std::unique_ptr<std::vector<char>> data(
                Compression::decompress(
                    compressed,
                    BaseChunk::makeCelled(m_metadata->schema()),
                    tail.numPoints));

        m_base.reset(
                static_cast<BaseChunk*>(
                    Chunk::create(
                        *m_builder,
                        bbox(),
                        0,
                        structure().baseIndexBegin(),
                        structure().baseIndexSpan(),
                        std::move(data)).release()));
    }

    if (m_metadata->structure().hasCold())
    {
        const auto idData(parse(m_endpoint.get("entwine-ids")));
        const auto strIds(extract<std::string>(idData));
        m_ids = std::accumulate(
                strIds.begin(),
                strIds.end(),
                std::set<Id>(),
                [](const std::string& s) { return Id(s) });
    }
    */
}

Reader::~Reader()
{ }

/*
Json::Value Reader::hierarchy(
        const BBox& qbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    checkQuery(depthBegin, depthEnd);
    return m_builder->hierarchy().query(qbox, depthBegin, depthEnd);
}

std::unique_ptr<Query> Reader::query(
        const Schema& schema,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const double scale,
        const Point offset)
{
    return query(schema, bbox(), depthBegin, depthEnd, scale, offset);
}

std::unique_ptr<Query> Reader::query(
        const Schema& schema,
        const BBox& qbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const double scale,
        const Point offset)
{
    checkQuery(depthBegin, depthEnd);

    BBox queryCube(qbox);

    if (!qbox.is3d())
    {
        // Make sure the query is 3D.
        queryCube = BBox(
                Point(qbox.min().x, qbox.min().y, bbox().min().z),
                Point(qbox.max().x, qbox.max().y, bbox().max().z),
                true);
    }

    return std::unique_ptr<Query>(
            new Query(
                *this,
                schema,
                m_cache,
                queryCube,
                depthBegin,
                depthEnd,
                scale,
                offset));
}

const BBox& Reader::bboxConforming() const
{
    return m_builder->bboxConforming();
}

const BBox& Reader::bbox() const            { return m_builder->bbox(); }
const Schema& Reader::schema() const        { return m_builder->schema(); }
const Structure& Reader::structure() const  { return m_builder->structure(); }
const std::string& Reader::srs() const      { return m_builder->srs(); }
std::string Reader::path() const            { return m_endpoint.root(); }

const BaseChunk* Reader::base() const { return m_base.get(); }
const arbiter::Endpoint& Reader::endpoint() const { return m_endpoint; }

std::size_t Reader::numPoints() const
{
    return m_builder->numPointsClone();
}
*/

} // namespace entwine


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

#include <entwine/compression/util.hpp>
#include <entwine/reader/query.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/manifest.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/stats.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>

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
    , m_bbox()
    , m_schema()
    , m_structure()
    , m_reprojection()
    , m_manifest()
    , m_stats()
    , m_base()
    , m_pointPool()
    , m_cache(cache)
    , m_srs()
    , m_ids()
{
    using namespace arbiter;

    {
        Json::Reader reader;

        const std::string metaString(endpoint.getSubpath("entwine"));

        Json::Value props;
        reader.parse(metaString, props, false);

        {
            const std::string err(reader.getFormattedErrorMessages());

            if (!err.empty())
            {
                throw std::runtime_error("Invalid JSON: " + err);
            }
        }

        m_bbox.reset(new BBox(props["bbox"]));
        m_schema.reset(new Schema(props["schema"]));
        m_pointPool.reset(new Pools(m_schema->pointSize()));
        m_structure.reset(new Structure(props["structure"], *m_bbox));
        if (props.isMember("reprojection"))
            m_reprojection.reset(new Reprojection(props["reprojection"]));
        m_manifest.reset(new Manifest(props["manifest"]));
        m_stats.reset(new Stats(props["stats"]));
        m_srs = props["srs"].asString();

        if (props.isMember("ids"))
        {
            const Json::Value& jsonIds(props["ids"]);

            if (jsonIds.isArray())
            {
                for (Json::ArrayIndex i(0); i < jsonIds.size(); ++i)
                {
                    m_ids.insert(Id(jsonIds[i].asString()));
                }
            }
            else
            {
                throw std::runtime_error("Meta member 'ids' is the wrong type");
            }
        }
    }

    {
        std::unique_ptr<std::vector<char>> data(
                new std::vector<char>(
                    endpoint.getSubpathBinary(
                        m_structure->baseIndexBegin().str())));

        m_base.reset(
                static_cast<BaseChunk*>(
                    Chunk::create(
                        *m_schema,
                        *m_bbox,
                        *m_structure,
                        *m_pointPool,
                        0,
                        m_structure->baseIndexBegin(),
                        m_structure->baseIndexSpan(),
                        std::move(data)).release()));
    }
}

Reader::~Reader()
{ }

std::unique_ptr<Query> Reader::query(
        const Schema& schema,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    return query(schema, *m_bbox, depthBegin, depthEnd);
}

std::unique_ptr<Query> Reader::query(
        const Schema& schema,
        const BBox& qbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    checkQuery(depthBegin, depthEnd);

    BBox normalBBox(qbox);

    if (!qbox.is3d())
    {
        normalBBox = BBox(
                Point(qbox.min().x, qbox.min().y, m_bbox->min().z),
                Point(qbox.max().x, qbox.max().y, m_bbox->max().z),
                true);
    }

    return std::unique_ptr<Query>(
            new Query(
                *this,
                *m_structure,
                schema,
                m_cache,
                normalBBox,
                depthBegin,
                depthEnd));
}

std::size_t Reader::numPoints() const
{
    return m_stats->getNumPoints();
}

} // namespace entwine


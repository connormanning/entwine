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
        std::shared_ptr<Cache> cache)
    : m_path(endpoint.root())
    , m_bbox()
    , m_schema()
    , m_structure()
    , m_reprojection()
    , m_manifest()
    , m_stats()
    , m_base()
    , m_cache(cache)
    , m_is3d(false)
    , m_srs()
    , m_ids()
{
    using namespace arbiter;

    if (!cache)
    {
        throw std::runtime_error("No cache supplied");
    }

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
        m_structure.reset(new Structure(props["structure"]));
        m_is3d = m_structure->is3d();
        if (props.isMember("reprojection"))
            m_reprojection.reset(new Reprojection(props["reprojection"]));
        m_manifest.reset(new Manifest(props["manifest"]));
        m_stats.reset(new Stats(props["stats"]));
        m_srs = props["srs"].asString();

        const Json::Value& jsonIds(props["ids"]);

        if (jsonIds.isArray())
        {
            std::set<Id>& ids(
                    m_ids.insert(
                        std::make_pair(
                            std::unique_ptr<Endpoint>(new Endpoint(endpoint)),
                            std::set<Id>())).first->second);

            for (std::size_t i(0); i < jsonIds.size(); ++i)
            {
                ids.insert(
                        Id(jsonIds[static_cast<Json::ArrayIndex>(i)]
                            .asString()));
            }
        }
        else if (jsonIds.isObject())
        {
            const auto subs(jsonIds.getMemberNames());

            for (const auto path : subs)
            {
                Endpoint sub(arbiter.getEndpoint(path));
                std::set<Id>& ids(
                        m_ids.insert(
                            std::make_pair(
                                std::unique_ptr<Endpoint>(new Endpoint(sub)),
                                std::set<Id>())).first->second);

                const Json::Value& subIds(jsonIds[path]);

                for (std::size_t i(0); i < subIds.size(); ++i)
                {
                    ids.insert(
                            Id(subIds[static_cast<Json::ArrayIndex>(i)]
                                .asString()));
                }
            }
        }
        else
        {
            throw std::runtime_error("Meta member 'ids' is the wrong type");
        }
    }

    {
        std::unique_ptr<std::vector<char>> data(
                new std::vector<char>(
                    endpoint.getSubpathBinary(
                        m_structure->baseIndexBegin().str())));

        m_base = ChunkReader::create(
                *m_schema,
                m_structure->baseIndexBegin(),
                m_structure->baseIndexSpan(),
                std::move(data));
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
        const BBox& bbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    checkQuery(depthBegin, depthEnd);

    if (m_is3d != bbox.is3d())
    {
        throw InvalidQuery("Wrong number of dimensions in query");
    }

    std::unique_ptr<Block> block(
            m_cache->acquire(
                m_path,
                traverse(bbox, depthBegin, depthEnd)));

    // Get the points selected by this query and all associated metadata.
    return runQuery(std::move(block), schema, bbox, depthBegin, depthEnd);
}

FetchInfoSet Reader::traverse(
        const BBox& queryBBox,
        const std::size_t depthBegin,
        const std::size_t depthEnd) const
{
    FetchInfoSet toFetch;

    if (
            m_structure->hasCold() &&
            (depthBegin >= m_structure->coldDepthBegin() ||
            !depthEnd ||
            depthEnd > m_structure->coldDepthBegin()))
    {
        const ChunkClimber c(*m_bbox, *m_structure);
        traverse(toFetch, c, queryBBox, depthBegin, depthEnd);
    }

    return toFetch;
}

void Reader::traverse(
        FetchInfoSet& toFetch,
        const ChunkClimber& c,
        const BBox& queryBBox,
        const std::size_t depthBegin,
        const std::size_t depthEnd) const
{
    if (!c.bbox().overlaps(queryBBox))
    {
        return;
    }

    const std::size_t chunkId(c.chunkId());
    const std::size_t depth(c.depth());

    if (chunkId)
    {
        if (arbiter::Endpoint* endpoint = getEndpoint(chunkId))
        {
            if (depth >= depthBegin && (depth < depthEnd || !depthEnd))
            {
                if (toFetch.size() + 1 >= m_cache->queryLimit())
                {
                    throw QueryLimitExceeded();
                }

                toFetch.insert(
                        FetchInfo(
                            *endpoint,
                            *m_schema,
                            chunkId,
                            m_structure->getInfo(chunkId).chunkPoints()));
            }
        }
        else
        {
            return;
        }
    }

    if (depth + 1 < depthEnd || !depthEnd)
    {
        if (
                m_structure->dynamicChunks() &&
                m_structure->sparseDepthBegin() &&
                depth >= m_structure->sparseDepthBegin())
        {
            traverse(toFetch, c.shimmy(), queryBBox, depthBegin, depthEnd);
        }
        else
        {
            traverse(toFetch, c.getNwd(), queryBBox, depthBegin, depthEnd);
            traverse(toFetch, c.getNed(), queryBBox, depthBegin, depthEnd);
            traverse(toFetch, c.getSwd(), queryBBox, depthBegin, depthEnd);
            traverse(toFetch, c.getSed(), queryBBox, depthBegin, depthEnd);

            if (!m_is3d) return;

            traverse(toFetch, c.getNwu(), queryBBox, depthBegin, depthEnd);
            traverse(toFetch, c.getNeu(), queryBBox, depthBegin, depthEnd);
            traverse(toFetch, c.getSwu(), queryBBox, depthBegin, depthEnd);
            traverse(toFetch, c.getSeu(), queryBBox, depthBegin, depthEnd);
        }
    }
}

std::unique_ptr<Query> Reader::runQuery(
        std::unique_ptr<Block> block,
        const Schema& schema,
        const BBox& qbox,
        std::size_t depthBegin,
        std::size_t depthEnd)
{
    std::unique_ptr<Query> query(new Query(*this, schema, std::move(block)));
    SplitClimber splitter(*m_structure, *m_bbox, qbox, depthBegin, depthEnd);

    bool terminate(false);

    // Get base data.
    if (depthBegin < m_structure->coldDepthBegin())
    {
        do
        {
            const std::size_t index(splitter.index());
            terminate = false;

            if (const char* pos = m_base->getData(index))
            {
                Point point(query->unwrapPoint(pos));

                if (Point::exists(point))
                {
                    if (qbox.contains(point))
                    {
                        query->insert(pos);
                    }
                }
                else
                {
                    terminate = true;
                }
            }
            else
            {
                throw std::runtime_error("Invalid base data");
            }
        }
        while (splitter.next(terminate));
    }

    // Get cold data.
    for (const auto& c : query->chunkMap())
    {
        if (c.second)
        {
            ChunkIter it(*c.second);

            do
            {
                const Point point(query->unwrapPoint(it.getData()));

                if (Point::exists(point) && qbox.contains(point))
                {
                    query->insert(it.getData());
                }
            }
            while (it.next());
        }
    }

    return query;
}

arbiter::Endpoint* Reader::getEndpoint(const std::size_t chunkId) const
{
    arbiter::Endpoint* endpoint(0);

    auto it(m_ids.begin());
    const auto end(m_ids.end());

    while (!endpoint && it != end)
    {
        if (it->second.count(chunkId))
        {
            endpoint = it->first.get();
        }

        ++it;
    }

    return endpoint;
}

std::size_t Reader::numPoints() const
{
    return m_stats->getNumPoints();
}

} // namespace entwine


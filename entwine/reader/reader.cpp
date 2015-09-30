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
        m_structure.reset(new Structure(props["structure"], *m_bbox));
        m_is3d = m_structure->is3d();
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
                std::set<Id>& ids(
                        m_ids.insert(
                            std::make_pair(
                                std::unique_ptr<Endpoint>(
                                    new Endpoint(endpoint)),
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
                                    std::unique_ptr<Endpoint>(
                                        new Endpoint(sub)),
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
    }

    {
        std::vector<char> data(
                endpoint.getSubpathBinary(m_structure->baseIndexBegin().str()));

        m_base.reset(
                static_cast<ContiguousChunk*>(
                    Chunk::create(
                        *m_schema,
                        m_structure->baseIndexBegin(),
                        m_structure->baseIndexSpan(),
                        data).release()));
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

    // TODO - Verify against structure.
    if (!qbox.is3d())
    {
        std::cout << "qbox: " << qbox << std::endl;
        throw InvalidQuery("Wrong number of dimensions in query");
    }

    return std::unique_ptr<Query>(
            new Query(
                *this,
                *m_structure,
                schema,
                m_cache,
                qbox,
                depthBegin,
                depthEnd));
}

arbiter::Endpoint* Reader::getEndpoint(const Id& chunkId) const
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


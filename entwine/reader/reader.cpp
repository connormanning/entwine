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
#include <entwine/reader/cache.hpp>
#include <entwine/reader/query.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/manifest.hpp>
#include <entwine/tree/registry.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/reprojection.hpp>
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
    , m_builder(new Builder(endpoint.type() + "://" + endpoint.root()))
    , m_base()
    , m_cache(cache)
    , m_ids(m_builder->registry().ids())
{
    using namespace arbiter;

    std::unique_ptr<std::vector<char>> data(
            new std::vector<char>(
                endpoint.getSubpathBinary(structure().baseIndexBegin().str())));

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

Reader::~Reader()
{ }

Json::Value Reader::hierarchy(
        const BBox& qbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    checkQuery(depthBegin, depthEnd);

    Json::Value json;
    doHierarchyLevel(json, qbox, depthBegin, depthEnd);
    return json;
}

void Reader::doHierarchyLevel(
        Json::Value& json,
        const BBox& qbox,
        const std::size_t depth,
        const std::size_t depthEnd,
        const std::string dir)
{
    const std::size_t nextDepth(depth + 1);

    std::unique_ptr<MetaQuery> query(
            new MetaQuery(
                *this,
                m_cache,
                qbox,
                depth,
                nextDepth));

    query->run();

    if (!query->numPoints()) return;

    Json::Value& self(dir.size() ? json[dir] : json);
    self["count"] = static_cast<Json::UInt64>(query->numPoints());

    if (nextDepth < depthEnd)
    {
        doHierarchyLevel(self, qbox.getNwu(), nextDepth, depthEnd, "nwu");
        doHierarchyLevel(self, qbox.getNwd(), nextDepth, depthEnd, "nwd");
        doHierarchyLevel(self, qbox.getNeu(), nextDepth, depthEnd, "neu");
        doHierarchyLevel(self, qbox.getNed(), nextDepth, depthEnd, "ned");

        doHierarchyLevel(self, qbox.getSwu(), nextDepth, depthEnd, "swu");
        doHierarchyLevel(self, qbox.getSwd(), nextDepth, depthEnd, "swd");
        doHierarchyLevel(self, qbox.getSeu(), nextDepth, depthEnd, "seu");
        doHierarchyLevel(self, qbox.getSed(), nextDepth, depthEnd, "sed");
    }
}

std::unique_ptr<Query> Reader::query(
        const Schema& schema,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const bool normalize)
{
    return query(schema, bbox(), depthBegin, depthEnd, normalize);
}

std::unique_ptr<Query> Reader::query(
        const Schema& schema,
        const BBox& qbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const bool normalize)
{
    checkQuery(depthBegin, depthEnd);

    BBox normalBBox(qbox);

    if (!qbox.is3d())
    {
        normalBBox = BBox(
                Point(qbox.min().x, qbox.min().y, bbox().min().z),
                Point(qbox.max().x, qbox.max().y, bbox().max().z),
                true);
    }

    return std::unique_ptr<Query>(
            new Query(
                *this,
                schema,
                m_cache,
                normalBBox,
                depthBegin,
                depthEnd,
                normalize));
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
    return m_builder->manifest().pointStats().inserts();
}

} // namespace entwine


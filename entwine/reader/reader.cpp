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
#include <entwine/tree/hierarchy.hpp>
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

    Json::Value& traverse(Json::Value& in, std::vector<std::string> keys)
    {
        Json::Value* json(&in);

        for (const auto& key : keys)
        {
            json = &((*json)[key]);
        }

        return *json;
    }

    std::vector<std::string> concat(
            const std::vector<std::string>& in,
            const std::string& add)
    {
        std::vector<std::string> out(in);
        out.push_back(add);
        return out;
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

    // OLD METHOD.
    Json::Value old;

    BoxMap grid;
    grid[qbox] = BoxInfo();
    doHierarchyLevel(old, qbox, grid, depthBegin, depthEnd);

    // NEW METHOD.
    Json::Value cur(m_builder->hierarchy().query(qbox, depthBegin, depthEnd));

    // COMPARE.
    std::cout << "OLD: " << old.toStyledString() << std::endl;
    std::cout << "NEW: " << cur.toStyledString() << std::endl;

    return cur;
}

void Reader::doHierarchyLevel(
        Json::Value& json,
        const BBox& qbox,
        BoxMap grid,
        const std::size_t depth,
        const std::size_t depthEnd)
{
    std::unique_ptr<MetaQuery> query(
            new MetaQuery(
                *this,
                m_cache,
                qbox,
                grid,
                depth));

    query->run();

    BoxMap next;
    const std::size_t nextDepth(depth + 1);

    for (const auto it : grid)
    {
        const auto& info(it.second);

        if (info.numPoints)
        {
            const auto& box(it.first);

            traverse(json, info.keys)["count"] =
                static_cast<Json::UInt64>(info.numPoints);

            if (nextDepth < depthEnd)
            {
                next[box.getNwu()] = BoxInfo(concat(info.keys, "nwu"));
                next[box.getNwd()] = BoxInfo(concat(info.keys, "nwd"));
                next[box.getNeu()] = BoxInfo(concat(info.keys, "neu"));
                next[box.getNed()] = BoxInfo(concat(info.keys, "ned"));
                next[box.getSwu()] = BoxInfo(concat(info.keys, "swu"));
                next[box.getSwd()] = BoxInfo(concat(info.keys, "swd"));
                next[box.getSeu()] = BoxInfo(concat(info.keys, "seu"));
                next[box.getSed()] = BoxInfo(concat(info.keys, "sed"));
            }
        }
    }

    if (nextDepth < depthEnd)
    {
        doHierarchyLevel(json, qbox, next, nextDepth, depthEnd);
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


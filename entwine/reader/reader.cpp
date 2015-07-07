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
#include <entwine/drivers/arbiter.hpp>
#include <entwine/drivers/source.hpp>
#include <entwine/reader/query.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/manifest.hpp>
#include <entwine/tree/roller.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/stats.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

namespace
{
    void checkQuery(std::size_t depthBegin, std::size_t depthEnd)
    {
        if (depthBegin >= depthEnd)
        {
            throw std::runtime_error("Invalid query depths");
        }
    }
}

Reader::Reader(Source source, Arbiter& arbiter, std::shared_ptr<Cache> cache)
    : m_path(source.path())
    , m_bbox()
    , m_schema()
    , m_structure()
    , m_reprojection()
    , m_manifest()
    , m_stats()
    , m_base()
    , m_cache(cache)
    , m_ids()
{
    if (!cache)
    {
        throw std::runtime_error("No cache supplied");
    }

    {
        Json::Reader reader;

        const std::string metaString(source.getAsString("entwine"));

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
        if (props.isMember("reprojection"))
            m_reprojection.reset(new Reprojection(props["reprojection"]));
        m_manifest.reset(new Manifest(props["manifest"]));
        m_stats.reset(new Stats(props["stats"]));

        const Json::Value& jsonIds(props["ids"]);

        if (jsonIds.isArray())
        {
            std::set<std::size_t>& ids(
                    m_ids.insert(
                        std::make_pair(
                            std::unique_ptr<Source>(new Source(source)),
                            std::set<std::size_t>())).first->second);

            for (std::size_t i(0); i < jsonIds.size(); ++i)
            {
                ids.insert(
                        jsonIds[static_cast<Json::ArrayIndex>(i)].asUInt64());
            }
        }
        else if (jsonIds.isObject())
        {
            const auto subs(jsonIds.getMemberNames());

            for (const auto path : subs)
            {
                Source subSrc(arbiter.getSource(path));
                std::set<std::size_t>& ids(
                        m_ids.insert(
                            std::make_pair(
                                std::unique_ptr<Source>(new Source(subSrc)),
                                std::set<std::size_t>())).first->second);

                const Json::Value& subIds(jsonIds[path]);

                for (std::size_t i(0); i < subIds.size(); ++i)
                {
                    ids.insert(
                            subIds[static_cast<Json::ArrayIndex>(i)]
                            .asUInt64());
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
                    source.get(
                        std::to_string(m_structure->baseIndexBegin()))));

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
    std::size_t tries(0);
    const Roller roller(*m_bbox, *m_structure);

    traverse(toFetch, tries, roller, queryBBox, depthBegin, depthEnd);

    return toFetch;
}

void Reader::traverse(
        FetchInfoSet& toFetch,
        std::size_t& tries,
        const Roller& r,
        const BBox& queryBBox,
        const std::size_t depthBegin,
        const std::size_t depthEnd) const
{
    if (!r.bbox().overlaps(queryBBox) || !m_structure->inRange(r.index()))
        return;

    const uint64_t index(r.index());
    const std::size_t depth(r.depth());

    if (
            m_structure->isWithinCold(index) &&
            depth >= depthBegin &&
            (depth < depthEnd || !depthEnd))
    {
        const std::size_t chunkId(getChunkId(index, depth));

        if (Source* source = getSource(chunkId))
        {
            if (toFetch.size() + 1 >= m_cache->queryLimit())
            {
                throw QueryLimitExceeded();
            }

            toFetch.insert(
                    FetchInfo(
                        *source,
                        *m_schema,
                        chunkId,
                        m_structure->getInfo(chunkId).chunkPoints()));
        }
        else
        {
            if (++tries > m_cache->queryLimit())
            {
                throw QueryLimitExceeded();
            }
        }
    }

    if (depth + 1 < depthEnd || !depthEnd)
    {
        traverse(toFetch, tries, r.getNw(), queryBBox, depthBegin, depthEnd);
        traverse(toFetch, tries, r.getNe(), queryBBox, depthBegin, depthEnd);
        traverse(toFetch, tries, r.getSw(), queryBBox, depthBegin, depthEnd);
        traverse(toFetch, tries, r.getSe(), queryBBox, depthBegin, depthEnd);
    }
}

std::unique_ptr<Query> Reader::runQuery(
        std::unique_ptr<Block> block,
        const Schema& schema,
        const BBox& bbox,
        std::size_t depthBegin,
        std::size_t depthEnd)
{
    std::vector<const char*> points;
    const Roller roller(*m_bbox, *m_structure);

    std::unique_ptr<Query> query(new Query(*this, schema, std::move(block)));
    runQuery(*query, roller, bbox, depthBegin, depthEnd);

    return query;
}

void Reader::runQuery(
        Query& query,
        const Roller& roller,
        const BBox& queryBBox,
        const std::size_t depthBegin,
        const std::size_t depthEnd) const
{
    if (!roller.bbox().overlaps(queryBBox))
    {
        return;
    }

    const uint64_t index(roller.index());
    const std::size_t depth(roller.depth());

    if (depth >= depthBegin && (depth < depthEnd || !depthEnd))
    {
        if (const char* pos = getPointPos(index, query.chunkMap()))
        {
            Point point(query.unwrapPoint(pos));

            if (Point::exists(point) && queryBBox.contains(point))
            {
                query.insert(pos);
            }
        }
    }

    if (depth + 1 < depthEnd || !depthEnd)
    {
        runQuery(query, roller.getNw(), queryBBox, depthBegin, depthEnd);
        runQuery(query, roller.getNe(), queryBBox, depthBegin, depthEnd);
        runQuery(query, roller.getSw(), queryBBox, depthBegin, depthEnd);
        runQuery(query, roller.getSe(), queryBBox, depthBegin, depthEnd);
    }
}

const char* Reader::getPointPos(
        const std::size_t index,
        const ChunkMap& chunkMap) const
{
    const char* pos(0);

    if (m_structure->isWithinBase(index))
    {
        pos = m_base->getData(index);
    }
    else if (m_structure->isWithinCold(index))
    {
        const std::size_t chunkId(
                getChunkId(
                    index,
                    ChunkInfo::calcDepth(m_structure->factor(), index)));

        auto it(chunkMap.find(chunkId));
        if (it != chunkMap.end())
        {
            pos = it->second->getData(index);
        }
        else
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_missing.insert(chunkId).second)
            {
                std::cout << "\tMissing ID (" << m_path << "):" << chunkId <<
                    std::endl;
            }
        }
    }

    return pos;
}

std::size_t Reader::getChunkId(
        const std::size_t index,
        const std::size_t depth) const
{
    const std::size_t baseChunkPoints(m_structure->baseChunkPoints());

    if (
            !m_structure->hasSparse() ||
            !m_structure->dynamicChunks() ||
            depth <= m_structure->sparseDepthBegin())
    {
        const std::size_t coldDelta(index - m_structure->coldIndexBegin());

        return
            m_structure->coldIndexBegin() +
            (coldDelta / baseChunkPoints) * baseChunkPoints;
    }
    else
    {
        const std::size_t dimensions(m_structure->dimensions());

        const std::size_t levelIndex(
                ChunkInfo::calcLevelIndex(dimensions, depth));

        const std::size_t sparseDepthCount(
                depth - m_structure->sparseDepthBegin());

        const std::size_t levelChunkPoints(
                baseChunkPoints *
                ChunkInfo::binaryPow(dimensions, sparseDepthCount));

        return
            levelIndex +
            ((index - levelIndex) / levelChunkPoints) * levelChunkPoints;
    }
}

Source* Reader::getSource(const std::size_t chunkId) const
{
    Source* source(0);

    auto it(m_ids.begin());
    const auto end(m_ids.end());

    while (!source && it != end)
    {
        if (it->second.count(chunkId))
        {
            source = it->first.get();
        }

        ++it;
    }

    return source;
}

std::size_t Reader::numPoints() const
{
    return m_stats->getNumPoints();
}

} // namespace entwine


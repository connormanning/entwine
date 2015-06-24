/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/reader.hpp>

#include <entwine/compression/util.hpp>
#include <entwine/drivers/arbiter.hpp>
#include <entwine/drivers/source.hpp>
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

Query::Query(
        Reader& reader,
        const Schema& outSchema,
        ChunkMap chunkMap)
    : m_reader(reader)
    , m_chunkMap(chunkMap)
    , m_outSchema(outSchema)
    , m_points()
    , m_table(m_reader.schema(), 0)
    , m_view(m_table)
{ }

void Query::addPoint(const char* pos)
{
    m_points.push_back(pos);
}

Point Query::unwrapPoint(const char* pos)
{
    m_table.setData(pos);

    return Point(
            m_view.getFieldAs<double>(pdal::Dimension::Id::X, 0),
            m_view.getFieldAs<double>(pdal::Dimension::Id::Y, 0),
            m_view.getFieldAs<double>(pdal::Dimension::Id::Z, 0));
}

std::size_t Query::size() const
{
    return m_points.size();
}

void Query::getPointAt(const std::size_t index, char* out)
{
    m_table.setData(m_points.at(index));

    for (const auto& dim : m_outSchema.dims())
    {
        m_view.getField(out, dim.id(), dim.type(), 0);
        out += dim.size();
    }
}

Reader::Reader(
        Source source,
        const std::size_t cacheSize,
        const std::size_t queryLimit,
        std::shared_ptr<Arbiter> arbiter)
    : m_bbox()
    , m_schema()
    , m_structure()
    , m_reprojection()
    , m_manifest()
    , m_stats()
    , m_ids()
    , m_arbiter(arbiter)
    , m_cacheSize(cacheSize)
    , m_queryLimit(queryLimit)
    , m_mutex()
    , m_cv()
    , m_base()
    , m_chunks()
    , m_outstanding()
    , m_accessList()
    , m_accessMap()
{
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
                Source subSrc(m_arbiter->getSource(path));
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

    // Warm up cache and get the chunks necessary for this query.
    const ChunkMap chunkMap(warm(traverse(bbox, depthBegin, depthEnd)));

    // Get the points selected by this query and all associated metadata.
    return runQuery(chunkMap, schema, bbox, depthBegin, depthEnd);
}

std::set<std::size_t> Reader::traverse(
        const BBox& queryBBox,
        const std::size_t depthBegin,
        const std::size_t depthEnd) const
{
    std::set<std::size_t> results;
    const Roller roller(*m_bbox, *m_structure);

    traverse(results, roller, queryBBox, depthBegin, depthEnd);

    return results;
}

void Reader::traverse(
        std::set<std::size_t>& toFetch,
        const Roller& roller,
        const BBox& queryBBox,
        const std::size_t depthBegin,
        const std::size_t depthEnd) const
{
    if (!roller.bbox().overlaps(queryBBox)) return;

    const uint64_t index(roller.index());
    const std::size_t depth(roller.depth());

    if (
            m_structure->isWithinCold(index) &&
            depth >= depthBegin &&
            (depth < depthEnd || !depthEnd))
    {
        toFetch.insert(getChunkId(index, depth));
        if (toFetch.size() > m_queryLimit) throw QueryLimitExceeded();
    }

    if (depth + 1 < depthEnd || !depthEnd)
    {
        traverse(toFetch, roller.getNw(), queryBBox, depthBegin, depthEnd);
        traverse(toFetch, roller.getNe(), queryBBox, depthBegin, depthEnd);
        traverse(toFetch, roller.getSw(), queryBBox, depthBegin, depthEnd);
        traverse(toFetch, roller.getSe(), queryBBox, depthBegin, depthEnd);
    }
}

ChunkMap Reader::warm(const std::set<std::size_t>& toFetch)
{
    ChunkMap results;
    results.insert(std::make_pair(m_structure->baseIndexBegin(), m_base.get()));

    std::mutex mutex;
    std::unique_ptr<Pool> pool(
            new Pool(std::min<std::size_t>(8, toFetch.size())));

    for (const std::size_t chunkId : toFetch)
    {
        pool->add([this, chunkId, &mutex, &results]()->void
        {
            std::lock_guard<std::mutex> lock(mutex);
            results.insert(std::make_pair(chunkId, fetch(chunkId)));
        });
    }

    pool->join();
    return results;
}

std::unique_ptr<Query> Reader::runQuery(
        const ChunkMap& chunkMap,
        const Schema& schema,
        const BBox& bbox,
        std::size_t depthBegin,
        std::size_t depthEnd)
{
    std::vector<const char*> points;
    const Roller roller(*m_bbox, *m_structure);

    std::unique_ptr<Query> query(new Query(*this, schema, chunkMap));
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
    if (!roller.bbox().overlaps(queryBBox)) return;

    const uint64_t index(roller.index());
    const std::size_t depth(roller.depth());

    if (depth >= depthBegin && (depth < depthEnd || !depthEnd))
    {
        if (const char* pos = getPointPos(index, query.chunkMap()))
        {
            Point point(query.unwrapPoint(pos));

            if (Point::exists(point) && queryBBox.contains(point))
            {
                query.addPoint(pos);
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
            throw std::runtime_error("Cache overrun or invalid point detected");
        }
    }

    return pos;
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

const ChunkReader* Reader::fetch(const std::size_t chunkId)
{
    ChunkReader* result(0);

    if (!m_structure->isWithinCold(chunkId))
    {
        throw std::runtime_error("ChunkId out of range for fetching.");
    }

    if (Source* source = getSource(chunkId))
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        const auto initialIt(m_chunks.find(chunkId));
        const bool has(initialIt != m_chunks.end());
        const bool awaiting(m_outstanding.count(chunkId));

        if (!has && !awaiting)
        {
            std::cout << "Fetching " << chunkId << std::endl;
            m_outstanding.insert(chunkId);

            lock.unlock();

            std::unique_ptr<std::vector<char>> data;

            try
            {
                data.reset(new std::vector<char>(
                            source->get(std::to_string(chunkId))));
            }
            catch (...)
            {
                lock.lock();
                m_outstanding.erase(chunkId);
                lock.unlock();
                m_cv.notify_all();
                throw std::runtime_error(
                        "Could not fetch chunk " + std::to_string(chunkId));
            }

            auto chunk(
                    ChunkReader::create(
                        *m_schema,
                        chunkId,
                        m_structure->getInfo(chunkId).chunkPoints(),
                        std::move(data)));

            lock.lock();

            assert(m_accessMap.size() == m_chunks.size());
            if (m_chunks.size() >= m_cacheSize)
            {
                const std::size_t expired(m_accessList.back());
                std::cout << "Erasing " << expired << std::endl;

                if (m_chunks.count(expired))
                {
                    m_chunks.erase(expired);
                }

                m_accessMap.erase(expired);
                m_accessList.pop_back();
            }

            m_outstanding.erase(chunkId);
            m_accessList.push_front(chunkId);
            m_accessMap[chunkId] = m_accessList.begin();

            const auto insertResult(
                    m_chunks.insert(std::make_pair(chunkId, std::move(chunk))));
            const auto it(insertResult.first);
            result = it->second.get();

            lock.unlock();
            m_cv.notify_all();
        }
        else if (has)
        {
            // Touching a previously fetched chunk.  Move it to the front of
            // the access list.
            m_accessList.splice(
                    m_accessList.begin(),
                    m_accessList,
                    m_accessMap.at(chunkId));

            result = initialIt->second.get();
        }
        else
        {
            // Another thread is already fetching this chunk.  Wait for it to
            // finish.  No need to splice the access list here, since the
            // fetching thread will place it at the front.
            m_cv.wait(lock, [this, chunkId]()->bool
            {
                return m_chunks.count(chunkId) || !m_outstanding.count(chunkId);
            });

            const auto it(m_chunks.find(chunkId));
            if (it != m_chunks.end())
            {
                result = it->second.get();
            }
            else
            {
                throw std::runtime_error(
                        "Could not fetch chunk " + std::to_string(chunkId));
            }
        }
    }

    return result;
}

std::size_t Reader::numPoints() const
{
    return m_stats->getNumPoints();
}

} // namespace entwine


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
#include <entwine/drivers/source.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/manifest.hpp>
#include <entwine/tree/roller.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/stats.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/single-point-table.hpp>
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

Reader::Reader(
        Source source,
        const std::size_t cacheSize,
        const std::size_t queryLimit)
    : m_bbox()
    , m_schema()
    , m_structure()
    , m_reprojection()
    , m_manifest()
    , m_stats()
    , m_ids()
    , m_source(source)
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

        const std::string metaString(m_source.getAsString("entwine"));

        Json::Value props;
        reader.parse(metaString, props, false);

        m_bbox.reset(new BBox(props["bbox"]));
        m_schema.reset(new Schema(props["schema"]));
        m_structure.reset(new Structure(props["structure"]));
        if (props.isMember("reprojection"))
            m_reprojection.reset(new Reprojection(props["reprojection"]));
        m_manifest.reset(new Manifest(props["manifest"]));
        m_stats.reset(new Stats(props["stats"]));

        const Json::Value& jsonIds(props["ids"]);

        if (!jsonIds.isArray())
        {
            throw std::runtime_error("Invalid saved state.");
        }

        for (std::size_t i(0); i < jsonIds.size(); ++i)
        {
            m_ids.insert(jsonIds[static_cast<Json::ArrayIndex>(i)].asUInt64());
        }
    }

    {
        std::unique_ptr<std::vector<char>> data(
                new std::vector<char>(
                    m_source.get(
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

std::vector<std::size_t> Reader::query(
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    return query(*m_bbox, depthBegin, depthEnd);
}

std::vector<std::size_t> Reader::query(
        const BBox& bbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    checkQuery(depthBegin, depthEnd);

    std::vector<std::size_t> results;
    Roller roller(*m_bbox);

    // Pre-warm cache with necessary chunks for this query.
    std::set<std::size_t> toFetch;
    traverse(toFetch, roller, bbox, depthBegin, depthEnd);
    warm(toFetch);

    // Get query results.
    query(roller, results, bbox, depthBegin, depthEnd);

    return results;
}

void Reader::traverse(
        std::set<std::size_t>& toFetch,
        const Roller& roller,
        const BBox& queryBBox,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    if (!roller.bbox().overlaps(queryBBox)) return;

    const uint64_t index(roller.index());
    const std::size_t depth(roller.depth());

    if (
            m_structure->isWithinCold(index) &&
            depth >= depthBegin &&
            (depth < depthEnd || !depthEnd))
    {
        toFetch.insert(getChunkId(index));

        if (toFetch.size() > m_queryLimit)
        {
            std::cout << "Query limit exceeded." << std::endl;
            throw std::runtime_error("Max query size exceeded");
        }
    }

    if (depth + 1 < depthEnd || !depthEnd)
    {
        traverse(toFetch, roller.getNw(), queryBBox, depthBegin, depthEnd);
        traverse(toFetch, roller.getNe(), queryBBox, depthBegin, depthEnd);
        traverse(toFetch, roller.getSw(), queryBBox, depthBegin, depthEnd);
        traverse(toFetch, roller.getSe(), queryBBox, depthBegin, depthEnd);
    }
}

void Reader::warm(const std::set<std::size_t>& toFetch)
{
    std::unique_ptr<Pool> pool(
            new Pool(std::min<std::size_t>(8, toFetch.size())));

    for (const std::size_t chunkId : toFetch)
    {
        pool->add([this, chunkId]()->void
        {
            fetch(chunkId);
        });
    }

    pool->join();
}

void Reader::query(
        const Roller& roller,
        std::vector<std::size_t>& results,
        const BBox& queryBBox,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    if (!roller.bbox().overlaps(queryBBox)) return;

    const uint64_t index(roller.index());
    const std::size_t depth(roller.depth());

    if (depth >= depthBegin && (depth < depthEnd || !depthEnd))
    {
        Point point(getPoint(index));
        if (Point::exists(point) && queryBBox.contains(point))
        {
            results.push_back(index);
        }
    }

    if (depth + 1 < depthEnd || !depthEnd)
    {
        query(roller.getNw(), results, queryBBox, depthBegin, depthEnd);
        query(roller.getNe(), results, queryBBox, depthBegin, depthEnd);
        query(roller.getSw(), results, queryBBox, depthBegin, depthEnd);
        query(roller.getSe(), results, queryBBox, depthBegin, depthEnd);
    }
}

std::vector<char> Reader::getPointData(
        const std::size_t index,
        const Schema& reqSchema)
{
    std::vector<char> schemaPoint;
    std::vector<char> nativePoint;

    std::unique_lock<std::mutex> lock(m_mutex);
    if (char* pos = getPointData(index))
    {
        nativePoint.assign(pos, pos + m_schema->pointSize());
    }
    lock.unlock();

    if (nativePoint.size())
    {
        schemaPoint.resize(reqSchema.pointSize());

        SinglePointTable table(*m_schema, nativePoint.data());
        LinkingPointView view(table);

        char* dst(schemaPoint.data());

        for (const auto& reqDim : reqSchema.dims())
        {
            view.getField(dst, reqDim.id(), reqDim.type(), 0);
            dst += reqDim.size();
        }
    }

    return schemaPoint;
}

Point Reader::getPoint(const std::size_t index)
{
    Point point;

    std::lock_guard<std::mutex> lock(m_mutex);
    if (char* pos = getPointData(index))
    {
        SinglePointTable table(*m_schema, pos);
        LinkingPointView view(table);

        point.x = view.getFieldAs<double>(pdal::Dimension::Id::X, 0);
        point.y = view.getFieldAs<double>(pdal::Dimension::Id::Y, 0);
    }

    return point;
}

char* Reader::getPointData(const std::size_t index)
{
    char* pos(0);

    if (m_structure->isWithinBase(index))
    {
        pos = m_base->getData(index);
    }
    else if (m_structure->isWithinCold(index))
    {
        const std::size_t chunkId(getChunkId(index));

        auto it(m_chunks.find(chunkId));
        if (it != m_chunks.end())
        {
            pos = it->second->getData(index);
        }
        else if (m_ids.count(chunkId))
        {
            std::cout << "Missing chunk " << chunkId << std::endl;
            std::cout << "Cache size: " << m_chunks.size() << std::endl;
            throw std::runtime_error("Cache overrun or invalid point detected");
        }
    }

    return pos;
}

std::size_t Reader::maxId() const
{
    return *m_ids.rbegin();
}

std::size_t Reader::getChunkId(const std::size_t index) const
{
    assert(m_structure->isWithinCold(index));

    const std::size_t firstChunk(m_structure->coldIndexBegin());
    const std::size_t chunkPoints(m_structure->chunkPoints());

    // Zero-based chunk number.
    const std::size_t chunkNum((index - firstChunk) / chunkPoints);
    return firstChunk + chunkNum * chunkPoints;
}

void Reader::fetch(const std::size_t chunkId)
{
    if (!m_structure->isWithinCold(chunkId) || !m_ids.count(chunkId))
    {
        return;
    }

    std::unique_lock<std::mutex> lock(m_mutex);

    const bool has(m_chunks.count(chunkId));
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
                        m_source.get(std::to_string(chunkId))));
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
                    m_structure->chunkPoints(),
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
        m_chunks.insert(std::make_pair(chunkId, std::move(chunk)));
        m_accessList.push_front(chunkId);
        m_accessMap[chunkId] = m_accessList.begin();

        lock.unlock();
        m_cv.notify_all();
    }
    else if (has)
    {
        // Touching a previously fetched chunk.  Move it to the front of the
        // access list.
        m_accessList.splice(
                m_accessList.begin(),
                m_accessList,
                m_accessMap.at(chunkId));
    }
    else
    {
        // Another thread is already fetching this chunk.  Wait for it to
        // finish.  No need to splice the access list here, since the fetching
        // thread will place it at the front.
        m_cv.wait(lock, [this, chunkId]()->bool
        {
            return m_chunks.count(chunkId) || !m_outstanding.count(chunkId);
        });

        if (!m_chunks.count(chunkId))
        {
            throw std::runtime_error(
                    "Could not fetch chunk " + std::to_string(chunkId));
        }
    }
}

std::size_t Reader::numPoints() const
{
    return m_stats->getNumPoints();
}

} // namespace entwine


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
#include <entwine/tree/roller.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/single-point-table.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

namespace
{
    void checkQuery(std::size_t depthBegin, std::size_t depthEnd)
    {
        if (depthEnd >= 25)
        {
            throw std::runtime_error("Over a quadrillion points unsupported");
        }

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
    : m_firstChunk(0)
    , m_chunkPoints(0)
    , m_ids()
    , m_bbox()
    , m_schema()
    , m_dimensions(0)
    , m_numPoints(0)
    , m_numTossed(0)
    , m_originList()
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
    Json::Reader reader;
    std::size_t numIds(0);

    {
        const std::string metaString(m_source.getAsString("entwine"));

        Json::Value meta;
        reader.parse(metaString, meta, false);

        m_firstChunk = meta["firstChunk"].asUInt64();
        m_chunkPoints = meta["chunkPoints"].asUInt64();

        m_bbox.reset(new BBox(BBox::fromJson(meta["bbox"])));
        m_schema.reset(new Schema(Schema::fromJson(meta["schema"])));
        m_dimensions = meta["dimensions"].asUInt64();
        m_numPoints = meta["numPoints"].asUInt64();
        m_numTossed = meta["numTossed"].asUInt64();
        numIds = meta["numIds"].asUInt64();
        const Json::Value& metaManifest(meta["manifest"]);

        for (Json::ArrayIndex i(0); i < metaManifest.size(); ++i)
        {
            m_originList.push_back(metaManifest[i].asString());
        }
    }

    {
        const std::string idsString(m_source.getAsString("ids"));

        Json::Value ids;
        reader.parse(idsString, ids, false);

        if (ids.size() != numIds)
        {
            throw std::runtime_error("ID count mismatch");
        }

        for (Json::ArrayIndex i(0); i < ids.size(); ++i)
        {
            m_ids.insert(ids[i].asUInt64());
        }
    }

    {
        std::vector<char> data(m_source.get("0"));

        const std::size_t baseSize(m_firstChunk * m_schema->pointSize());
        m_base = Compression::decompress(data, *m_schema, baseSize);
    }
}

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
    std::unique_ptr<Pool> pool(new Pool(16));
    std::set<std::size_t> fetching;
    warm(roller, *pool, fetching, bbox, depthBegin, depthEnd);
    pool->join();

    // Get query results.
    query(roller, results, bbox, depthBegin, depthEnd);

    return results;
}

void Reader::warm(
        const Roller& roller,
        Pool& pool,
        std::set<std::size_t>& fetching,
        const BBox& queryBBox,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    if (!roller.bbox().overlaps(queryBBox)) return;

    const uint64_t index(roller.index());
    const std::size_t depth(roller.depth());

    if (
            index >= m_firstChunk &&    // Base data always exists.
            depth >= depthBegin &&
            (depth < depthEnd || !depthEnd))
    {
        const std::size_t chunkId(getChunkId(index));

        if (!fetching.count(chunkId))
        {
            fetching.insert(chunkId);

            if (fetching.size() > m_queryLimit)
            {
                std::cout << "Query limit exceeded.  Joining..." << std::endl;
                pool.join();
                std::cout << "Joined.  Throwing." << std::endl;
                throw std::runtime_error("Max query size exceeded");
            }

            pool.add([this, chunkId]()->void
            {
                fetch(chunkId);
            });
        }
    }

    if (depth + 1 < depthEnd || !depthEnd)
    {
        warm(roller.getNw(), pool, fetching, queryBBox, depthBegin, depthEnd);
        warm(roller.getNe(), pool, fetching, queryBBox, depthBegin, depthEnd);
        warm(roller.getSw(), pool, fetching, queryBBox, depthBegin, depthEnd);
        warm(roller.getSe(), pool, fetching, queryBBox, depthBegin, depthEnd);
    }
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
        if (queryBBox.contains(point))
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
    Point point(INFINITY, INFINITY);

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

    if (index < m_firstChunk)
    {
        pos = m_base->data() + index * m_schema->pointSize();
    }
    else
    {
        const std::size_t chunkId(getChunkId(index));
        const std::size_t offset((index - chunkId) * m_schema->pointSize());

        auto it(m_chunks.find(chunkId));
        if (it != m_chunks.end())
        {
            pos = it->second->data() + offset;
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
    if (index < m_firstChunk) return 0;

    // Zero-based chunk number.
    const std::size_t chunkNum((index - m_firstChunk) / m_chunkPoints);
    return m_firstChunk + chunkNum * m_chunkPoints;
}

void Reader::fetch(const std::size_t chunkId)
{
    if (chunkId > maxId() || chunkId < m_firstChunk || !m_ids.count(chunkId))
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

        std::vector<char> data;

        try
        {
            data = m_source.get(std::to_string(chunkId));
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

        const std::size_t chunkSize(m_chunkPoints * m_schema->pointSize());

        std::unique_ptr<std::vector<char>> chunk(
                Compression::decompress(data, *m_schema, chunkSize));

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

} // namespace entwine


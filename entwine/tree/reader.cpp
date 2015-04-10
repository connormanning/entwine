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
#include <entwine/tree/roller.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/single-point-table.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

Reader::Reader(const S3Info& s3Info, std::size_t cacheSize)
    : m_firstChunk(0)
    , m_chunkPoints(0)
    , m_ids()
    , m_bbox()
    , m_schema()
    , m_dimensions(0)
    , m_numPoints(0)
    , m_numTossed(0)
    , m_originList()
    , m_pool(new Pool(32))
    , m_s3(new S3(s3Info))
    , m_cacheSize(cacheSize)
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
        HttpResponse res(m_s3->get("entwine"));

        if (res.code() != 200)
        {
            throw std::runtime_error("Couldn't fetch meta");
        }

        Json::Value meta;
        std::string metaString(res.data()->begin(), res.data()->end());

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
        HttpResponse res(m_s3->get("ids"));

        if (res.code() != 200)
        {
            throw std::runtime_error("Couldn't fetch ids");
        }

        Json::Value ids;
        std::string idsString(res.data()->begin(), res.data()->end());

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
        HttpResponse res(m_s3->get("0"));

        if (res.code() != 200)
        {
            throw std::runtime_error("Couldn't fetch base data");
        }

        const std::size_t baseSize(m_firstChunk * m_schema->pointSize());
        m_base = Compression::decompress(*res.data(), *m_schema, baseSize);
    }
}

Reader::~Reader()
{
    for (auto c : m_chunks)
    {
        delete c.second;
    }
}

std::vector<std::size_t> Reader::query(
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    if (depthEnd >= 25)
    {
        throw std::runtime_error("Over a quadrillion points unsupported");
    }

    if (depthBegin > depthEnd)
    {
        throw std::runtime_error("Invalid query depths");
    }

    std::vector<std::size_t> results;
    Roller roller(*m_bbox);

    query(roller, results, depthBegin, depthEnd);

    return results;
}

std::vector<std::size_t> Reader::query(
        const BBox& bbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    if (depthEnd >= 25)
    {
        throw std::runtime_error("Over a quadrillion points unsupported");
    }

    if (depthBegin > depthEnd)
    {
        throw std::runtime_error("Invalid query depths");
    }

    std::vector<std::size_t> results;
    Roller roller(*m_bbox);

    query(roller, results, bbox, depthBegin, depthEnd);

    return results;
}

void Reader::query(
        const Roller& roller,
        std::vector<std::size_t>& results,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    const uint64_t index(roller.pos());
    const std::size_t depth(roller.depth());

    if (hasPoint(index))
    {
        if ((depth >= depthBegin) && (depth < depthEnd || !depthEnd))
        {
            results.push_back(index);
        }

        if (depth + 1 < depthEnd || !depthEnd)
        {
            query(roller.getNw(), results, depthBegin, depthEnd);
            query(roller.getNe(), results, depthBegin, depthEnd);
            query(roller.getSw(), results, depthBegin, depthEnd);
            query(roller.getSe(), results, depthBegin, depthEnd);
        }
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

    const uint64_t index(roller.pos());
    const std::size_t depth(roller.depth());

    if (hasPoint(index))
    {
        if (
                (depth >= depthBegin) && (depth < depthEnd || !depthEnd) &&
                queryBBox.contains(getPoint(index)))
        {
            results.push_back(index);
        }

        if (depth + 1 < depthEnd || !depthEnd)
        {
            query(roller.getNw(), results, queryBBox, depthBegin, depthEnd);
            query(roller.getNe(), results, queryBBox, depthBegin, depthEnd);
            query(roller.getSw(), results, queryBBox, depthBegin, depthEnd);
            query(roller.getSe(), results, queryBBox, depthBegin, depthEnd);
        }
    }
}

std::vector<char> Reader::getPointData(
        const std::size_t index,
        const Schema& reqSchema)
{
    std::vector<char> schemaPoint;
    std::vector<char> nativePoint(getPointData(index));

    if (nativePoint.size())
    {
        schemaPoint.resize(reqSchema.pointSize());

        SinglePointTable table(*m_schema, nativePoint.data());
        LinkingPointView view(table);

        char* pos(schemaPoint.data());

        for (const auto& reqDim : reqSchema.dims())
        {
            view.getField(pos, reqDim.id(), reqDim.type(), 0);
            pos += reqDim.size();
        }
    }

    return schemaPoint;
}

bool Reader::hasPoint(const std::size_t index)
{
    if (index >= m_firstChunk)
    {
        const std::size_t chunk(
                m_firstChunk +
                ((index - m_firstChunk) / m_chunkPoints) * m_chunkPoints);
        if (!m_ids.count(chunk))
        {
            std::cout << "No good: " << chunk << std::endl;
            return false;
        }
    }

    return Point::exists(getPoint(index));
}

Point Reader::getPoint(const std::size_t index)
{
    Point point(INFINITY, INFINITY);

    char* pos(0);

    if (index < m_firstChunk)
    {
        pos = m_base->data() + index * m_schema->pointSize();
    }
    else
    {
        const std::size_t chunk(
                m_firstChunk +
                ((index - m_firstChunk) / m_chunkPoints) * m_chunkPoints);
        const std::size_t offset(
                ((index - m_firstChunk) % m_chunkPoints) *
                    m_schema->pointSize());

        // TODO Some highly unsafe/optimistic stuff here for cache management.
        // One-off testing code only.
        std::unique_lock<std::mutex> lock(m_mutex);
        if (!m_chunks.count(chunk) && !m_outstanding.count(chunk))
        {
            std::cout << "Fetching " << chunk << std::endl;

            if (m_accessMap.size() >= m_cacheSize)
            {
                const std::size_t expired(m_accessList.back());
                std::cout << "Erasing " << expired << std::endl;

                if (m_chunks.count(expired))
                {
                    if (m_chunks[expired]) delete m_chunks[expired];
                    m_chunks.erase(expired);
                }

                m_accessMap.erase(expired);
                m_accessList.pop_back();
            }

            m_accessList.push_front(chunk);
            m_accessMap[chunk] = m_accessList.begin();

            m_outstanding.insert(chunk);
            lock.unlock();

            HttpResponse res(m_s3->get(std::to_string(chunk)));

            if (res.code() != 200)
            {
                throw std::runtime_error("Couldn't fetch chunk");
            }

            const std::size_t chunkSize(
                m_chunkPoints * m_schema->pointSize());

            lock.lock();
            m_chunks[chunk] = Compression::decompress(
                    *res.data(),
                    *m_schema,
                    chunkSize).release();
            m_outstanding.erase(chunk);
        }
        else if (m_chunks.count(chunk))
        {
            // Move chunk this to the front of the access list.
            //
            // Don't do this splice if the chunk was outstanding earlier, since
            // the fetching thread will already do that.
            m_accessList.splice(
                    m_accessList.begin(),
                    m_accessList,
                    m_accessMap.at(chunk));
        }

        m_cv.wait(lock, [this, chunk]()->bool
        {
            return m_chunks.count(chunk);
        });

        pos = m_chunks.at(chunk)->data() + offset;
    }

    m_cv.notify_all();

    SinglePointTable table(*m_schema, pos);
    LinkingPointView view(table);

    point.x = view.getFieldAs<double>(pdal::Dimension::Id::X, 0);
    point.y = view.getFieldAs<double>(pdal::Dimension::Id::Y, 0);

    return point;
}

std::vector<char> Reader::getPointData(const std::size_t index)
{
    char* pos(0);

    if (index < m_firstChunk)
    {
        pos = m_base->data() + index * m_schema->pointSize();
    }
    else
    {
        const std::size_t chunk(
                m_firstChunk +
                ((index - m_firstChunk) / m_chunkPoints) * m_chunkPoints);
        const std::size_t offset(
                ((index - m_firstChunk) % m_chunkPoints) *
                    m_schema->pointSize());

        pos = m_chunks.at(chunk)->data() + offset;
    }

    return std::vector<char>(pos, pos + m_schema->pointSize());
}

} // namespace entwine


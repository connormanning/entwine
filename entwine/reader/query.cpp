/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/reader/query.hpp>

#include <algorithm>
#include <iterator>
#include <limits>

#include <pdal/util/Utils.hpp>

#include <entwine/reader/cache.hpp>
#include <entwine/reader/chunk-reader.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/types/dir.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/tube.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    std::size_t fetchesPerIteration(4);
}

Query::Query(
        const Reader& reader,
        const Schema& schema,
        const Json::Value& filter,
        Cache& cache,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const Point* scale,
        const Point* offset)
    : Query(
            reader,
            schema,
            filter,
            cache,
            Bounds::everything(),
            depthBegin,
            depthEnd,
            scale,
            offset)
{ }

Query::Query(
        const Reader& reader,
        const Schema& schema,
        const Json::Value& filter,
        Cache& cache,
        const Bounds& queryBounds,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const Point* scale,
        const Point* offset)
    : m_reader(reader)
    , m_structure(m_reader.metadata().structure())
    , m_cache(cache)
    , m_delta(Delta::maybeCreate(scale, offset))
    , m_queryBounds(queryBounds)
    , m_depthBegin(depthBegin)
    , m_depthEnd(depthEnd)
    , m_chunks()
    , m_block()
    , m_chunkReaderIt()
    , m_numPoints(0)
    , m_base(true)
    , m_done(false)
    , m_outSchema(schema)
    , m_table(m_reader.metadata().schema())
    , m_pointRef(m_table, 0)
    , m_filter(m_reader.metadata(), m_queryBounds, filter)
{
    if (!m_depthEnd || m_depthEnd > m_structure.coldDepthBegin())
    {
        QueryChunkState chunkState(m_structure, m_reader.metadata().bounds());
        getFetches(chunkState);
        std::cout << "Fetches: " << m_chunks.size() << std::endl;
    }
}

void Query::getFetches(const QueryChunkState& chunkState)
{
    if (!m_filter.check(chunkState.bounds())) return;

    if (
            chunkState.depth() >= m_depthBegin &&
            chunkState.depth() >= m_structure.coldDepthBegin() &&
            m_reader.exists(chunkState.chunkId()))
    {
        m_chunks.emplace(
                m_reader,
                chunkState.chunkId(),
                chunkState.pointsPerChunk(),
                chunkState.depth());
    }
    else if (
            chunkState.depth() >= m_structure.coldDepthBegin() &&
            !m_reader.exists(chunkState.chunkId()))
    {
        return;
    }

    if (chunkState.depth() + 1 < m_depthEnd)
    {
        if (chunkState.allDirections())
        {
            for (std::size_t i(0); i < 4; ++i)
            {
                getFetches(chunkState.getClimb(toDir(i)));
            }
        }
        else
        {
            getFetches(chunkState.getClimb());
        }
    }
}

bool Query::next(std::vector<char>& buffer)
{
    if (m_done) throw std::runtime_error("Called next after query completed");

    if (m_base)
    {
        m_base = false;

        if (m_reader.base())
        {
            PointState pointState(m_structure, m_reader.metadata().bounds());
            getBase(buffer, pointState);
        }

        if (buffer.empty())
        {
            if (m_chunks.empty()) m_done = true;
            else getChunked(buffer);
        }
    }
    else
    {
        getChunked(buffer);
    }

    return !m_done;
}

void Query::getBase(std::vector<char>& buffer, const PointState& pointState)
{
    if (!m_queryBounds.overlaps(pointState.bounds(), true)) return;

    if (pointState.depth() >= m_structure.baseDepthBegin())
    {
        const auto& cell(m_reader.base()->getTubeData(pointState.index()));
        if (cell.empty()) return;

        if (pointState.depth() >= m_depthBegin)
        {
            for (const PointInfo& pointInfo : cell)
            {
                if (processPoint(buffer, pointInfo)) ++m_numPoints;
            }
        }
    }

    if (
            pointState.depth() + 1 < m_structure.baseDepthEnd() &&
            pointState.depth() + 1 < m_depthEnd)
    {
        for (std::size_t i(0); i < dirHalfEnd(); ++i)
        {
            getBase(buffer, pointState.getClimb(toDir(i)));
        }
    }
}

void Query::getChunked(std::vector<char>& buffer)
{
    if (!m_block)
    {
        if (m_chunks.size())
        {
            const auto begin(m_chunks.begin());
            auto end(m_chunks.begin());
            std::advance(end, std::min(fetchesPerIteration, m_chunks.size()));

            FetchInfoSet subset(begin, end);
            m_block = m_cache.acquire(m_reader.path(), subset);
            m_chunks.erase(begin, end);

            if (m_block) m_chunkReaderIt = m_block->chunkMap().begin();
        }
    }

    if (m_block)
    {
        if (const ChunkReader* cr = m_chunkReaderIt->second)
        {
            ChunkReader::QueryRange range(cr->candidates(m_queryBounds));
            auto it(range.begin);

            while (it != range.end)
            {
                if (processPoint(buffer, it->second)) ++m_numPoints;
                ++it;
            }

            if (++m_chunkReaderIt == m_block->chunkMap().end())
            {
                m_block.reset();
            }
        }
        else
        {
            throw std::runtime_error("Reservation failure");
        }
    }

    m_done = !m_block && m_chunks.empty();
}

bool Query::processPoint(std::vector<char>& buffer, const PointInfo& info)
{
    if (m_queryBounds.contains(info.point()))
    {
        m_table.setPoint(info.data());

        if (!m_filter.check(m_pointRef)) return false;

        buffer.resize(buffer.size() + m_outSchema.pointSize(), 0);
        char* pos(buffer.data() + buffer.size() - m_outSchema.pointSize());

        std::size_t dimNum(0);
        const auto& mid(m_reader.metadata().bounds().mid());

        for (const auto& dim : m_outSchema.dims())
        {
            // Subtract one to ignore Dimension::Id::Unknown.
            dimNum = pdal::Utils::toNative(dim.id()) - 1;

            if (m_delta && dimNum < 3)
            {
                double d(m_pointRef.getFieldAs<double>(dim.id()));

                // Center the point around the origin, scale it, then un-center
                // it and apply the user's offset from the origin bounds center.
                d = Point::scale(
                        d,
                        mid[dimNum],
                        m_delta->scale()[dimNum],
                        m_delta->offset()[dimNum]);

                switch (dim.type())
                {
                    case pdal::Dimension::Type::Double:
                        std::memcpy(pos, &d, 8);
                        break;
                    case pdal::Dimension::Type::Float:
                        setSpatial<float>(pos, d);
                        break;
                    case pdal::Dimension::Type::Unsigned8:
                        setSpatial<uint8_t>(pos, d);
                        break;
                    case pdal::Dimension::Type::Signed8:
                        setSpatial<int8_t>(pos, d);
                        break;
                    case pdal::Dimension::Type::Unsigned16:
                        setSpatial<uint16_t>(pos, d);
                        break;
                    case pdal::Dimension::Type::Signed16:
                        setSpatial<int16_t>(pos, d);
                        break;
                    case pdal::Dimension::Type::Unsigned32:
                        setSpatial<uint32_t>(pos, d);
                        break;
                    case pdal::Dimension::Type::Signed32:
                        setSpatial<int32_t>(pos, d);
                        break;
                    case pdal::Dimension::Type::Unsigned64:
                        setSpatial<uint64_t>(pos, d);
                        break;
                    case pdal::Dimension::Type::Signed64:
                        setSpatial<int64_t>(pos, d);
                        break;
                    default:
                        break;
                }
            }
            else
            {
                m_pointRef.getField(pos, dim.id(), dim.type());
            }

            pos += dim.size();
        }

        return true;
    }
    else
    {
        return false;
    }
}

} // namespace entwine


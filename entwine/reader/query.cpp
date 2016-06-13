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

#include <iterator>

#include <entwine/reader/cache.hpp>
#include <entwine/reader/chunk-reader.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/types/dir.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/tube.hpp>

namespace entwine
{

namespace
{
    std::size_t fetchesPerIteration(4);
}

Query::Query(
        const Reader& reader,
        const Schema& schema,
        Cache& cache,
        const BBox& qbox,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const double scale,
        const Point& offset)
    : m_reader(reader)
    , m_structure(m_reader.metadata().structure())
    , m_cache(cache)
    , m_qbox(qbox)
    , m_depthBegin(depthBegin)
    , m_depthEnd(depthEnd)
    , m_chunks()
    , m_block()
    , m_chunkReaderIt()
    , m_numPoints(0)
    , m_base(true)
    , m_done(false)
    , m_outSchema(schema)
    , m_scale(scale)
    , m_offset(offset)
    , m_table(m_reader.metadata().schema())
    , m_pointRef(m_table, 0)
{
    if (!m_depthEnd || m_depthEnd > m_structure.coldDepthBegin())
    {
        ChunkState chunkState(m_structure, m_reader.metadata().bbox());
        getFetches(chunkState);
    }
}

void Query::getFetches(const ChunkState& chunkState)
{
    if (!m_qbox.overlaps(chunkState.bbox(), true)) return;

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
            PointState pointState(m_structure, m_reader.metadata().bbox());
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
    if (!m_qbox.overlaps(pointState.bbox(), true)) return;

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
        // Always climb in 2d.
        for (std::size_t i(0); i < 4; ++i)
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
            ChunkReader::QueryRange range(cr->candidates(m_qbox));
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
    if (m_qbox.contains(info.point()))
    {
        buffer.resize(buffer.size() + m_outSchema.pointSize(), 0);
        char* pos(buffer.data() + buffer.size() - m_outSchema.pointSize());

        m_table.setPoint(info.data());
        bool isX(false), isY(false), isZ(false);

        for (const auto& dim : m_outSchema.dims())
        {
            isX = dim.id() == pdal::Dimension::Id::X;
            isY = dim.id() == pdal::Dimension::Id::Y;
            isZ = dim.id() == pdal::Dimension::Id::Z;

            if (isX || isY || isZ)
            {
                double d(m_pointRef.getFieldAs<double>(dim.id()));

                if (isX)        d -= m_offset.x;
                else if (isY)   d -= m_offset.y;
                else            d -= m_offset.z;

                if (m_scale) d /= m_scale;

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


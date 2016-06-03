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
        SplitClimber splitter(
                m_structure,
                m_reader.metadata().bbox(),
                m_qbox,
                m_depthBegin,
                m_depthEnd,
                true);

        bool terminate(false);

        do
        {
            terminate = false;
            const Id& chunkId(splitter.index());

            if (m_reader.exists(chunkId))
            {
                m_chunks.insert(
                        FetchInfo(
                            m_reader,
                            chunkId,
                            m_structure.getInfo(chunkId).pointsPerChunk(),
                            splitter.depth()));
            }
            else
            {
                terminate = true;
            }
        }
        while (splitter.next(terminate));
    }
}

bool Query::next(std::vector<char>& buffer)
{
    if (m_done) throw std::runtime_error("Called next after query completed");

    if (m_base)
    {
        m_base = false;

        if (!getBase(buffer))
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

bool Query::getBase(std::vector<char>& buffer)
{
    const std::size_t startNumPoints(m_numPoints);
    auto dataExisted([this, startNumPoints]()
    {
        return m_numPoints != startNumPoints;
    });

    if (
            m_reader.base() &&
            m_depthBegin < m_structure.baseDepthEnd() &&
            m_depthEnd   > m_structure.baseDepthBegin())
    {
        const BaseChunkReader& base(*m_reader.base());
        bool terminate(false);
        SplitClimber splitter(
                m_structure,
                m_reader.metadata().bbox(),
                m_qbox,
                m_depthBegin,
                std::min(m_depthEnd, m_structure.baseDepthEnd()));

        if (splitter.index() < m_structure.baseIndexBegin())
        {
            return dataExisted();
        }

        do
        {
            terminate = false;
            const auto& tube(base.getTubeData(splitter.index()));

            if (tube.empty())
            {
                terminate = true;
            }
            else
            {
                for (const PointInfo& pointInfo : tube)
                {
                    if (processPoint(buffer, pointInfo))
                    {
                        ++m_numPoints;
                    }
                }
            }
        }
        while (splitter.next(terminate));
    }

    return dataExisted();
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


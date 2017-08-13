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
#include <limits>

#include <pdal/util/Utils.hpp>

#include <entwine/reader/cache.hpp>
#include <entwine/reader/reader.hpp>
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
    std::size_t fetchesPerIteration(6);
    std::size_t minBytesPerIteration(1024 * 1024);
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
    , m_queryBounds(
            queryBounds.intersection(m_reader.metadata().boundsScaledCubic()))
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
    , m_filter(m_reader.metadata(), m_queryBounds, filter, m_delta.get())
{
    if (!m_depthEnd || m_depthEnd > m_structure.coldDepthBegin())
    {
        QueryChunkState chunkState(
                m_structure,
                m_reader.metadata().boundsScaledCubic());
        getFetches(chunkState);
    }

    const Schema& s(m_reader.metadata().schema());
    for (auto& dim : m_outSchema.dims())
    {
        if (!s.contains(dim.name()))
        {
            if (m_reader.dimMap().count(dim.name()))
            {
                const std::string name(m_reader.dimMap().at(dim.name()));
                m_extraNames[dim.name()] = name;

                m_baseExtra = &m_reader.base()->extra(
                        name,
                        m_reader.extras().at(name));

                dim.setId(m_baseExtra->schema().find(dim.name()).id());
            }
        }
    }

    if (m_outSchema.contains("Mask"))
    {
        m_maskId = makeUnique<pdal::Dimension::Id>(
                m_outSchema.find("Mask").id());
    }
}

void Query::getFetches(const QueryChunkState& chunkState)
{
    if (!m_filter.check(chunkState.bounds())) return;

    if (
            chunkState.depth() >= m_depthBegin &&
            chunkState.depth() >= m_structure.coldDepthBegin() &&
            m_reader.exists(chunkState))
    {
        m_chunks.emplace(
                m_reader,
                chunkState.chunkId(),
                chunkState.bounds(),
                chunkState.depth());
    }
    else if (
            chunkState.depth() >= m_structure.coldDepthBegin() &&
            !m_reader.exists(chunkState))
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

    const std::size_t startSize(buffer.size());

    while (!m_done && buffer.size() - startSize < minBytesPerIteration)
    {
        if (m_base)
        {
            for (const auto& p : m_extraNames)
            {
                const std::string& dimName(p.first);
                const std::string& setName(p.second);
                m_baseExtras[dimName] = &m_reader.base()->extra(
                        setName,
                        m_reader.extras().at(setName));
            }


            m_base = false;

            if (m_reader.base())
            {
                PointState pointState(
                        m_structure,
                        m_reader.metadata().boundsScaledCubic());
                getBase(buffer, pointState);

                if (m_chunks.empty()) m_done = true;
            }
        }
        else
        {
            getChunked(buffer);
        }
    }

    return !m_done;
}

void Query::getBase(std::vector<char>& buffer, const PointState& pointState)
{
    if (!m_queryBounds.overlaps(pointState.bounds(), true)) return;

    if (pointState.depth() >= m_structure.baseDepthBegin())
    {
        const auto& cell(m_reader.base()->tubeData(pointState.index()));
        if (cell.empty()) return;

        if (pointState.depth() >= m_depthBegin)
        {
            for (const PointInfo& pointInfo : cell)
            {
                if (processPoint(buffer, pointInfo, pointState.depth()))
                {
                    ++m_numPoints;
                }
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
            m_extras.clear();
            for (const auto& p : m_extraNames)
            {
                const std::string& dimName(p.first);
                const std::string& setName(p.second);
                m_extras[dimName] = &cr->extra(
                        setName,
                        m_reader.extras().at(setName));
            }

            ChunkReader::QueryRange range(cr->candidates(m_queryBounds));
            auto it(range.begin);

            while (it != range.end)
            {
                if (processPoint(buffer, *it)) ++m_numPoints;
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

void Query::write(const std::string name, const std::vector<char>& data)
{
    if (m_done) throw std::runtime_error("Called next after query completed");

    m_name = name;
    m_data = &data;
    m_pos = data.data();

    m_writeTable = makeUnique<BinaryPointTable>(m_outSchema);
    m_writeRef = makeUnique<pdal::PointRef>(*m_writeTable, 0);

    while (!m_done)
    {
        if (m_base)
        {
            m_base = false;

            if (m_reader.base())
            {
                // TODO Very temporary.
                const_cast<Reader&>(m_reader).addExtra(
                        m_name,
                        m_outSchema.filter("Mask"));
                m_baseExtra = &m_reader.base()->extra(
                        m_name,
                        m_outSchema.filter("Mask"));
                PointState pointState(
                        m_structure,
                        m_reader.metadata().boundsScaledCubic());
                writeBase(pointState);

                if (m_chunks.empty()) m_done = true;
            }
        }
        else
        {
            writeChunked();
        }
    }
}

void Query::writeBase(const PointState& pointState)
{
    if (!m_queryBounds.overlaps(pointState.bounds(), true)) return;

    if (pointState.depth() >= m_structure.baseDepthBegin())
    {
        const auto& cell(m_reader.base()->tubeData(pointState.index()));
        if (cell.empty()) return;

        if (pointState.depth() >= m_depthBegin)
        {
            for (const PointInfo& info : cell)
            {
                if (m_queryBounds.contains(info.point()))
                {
                    m_table.setPoint(info.data());
                    m_writeTable->setPoint(m_pos);
                    if (m_filter.check(m_pointRef))
                    {
                        if (!m_maskId || m_writeRef->getFieldAs<bool>(*m_maskId))
                        {
                            m_writeRef->getPackedData(
                                    m_baseExtra->dimTypeList(),
                                    m_baseExtra->get(
                                        pointState.depth(),
                                        info.offset()));
                        }
                        m_pos += m_outSchema.pointSize();
                        ++m_numPoints;
                    }
                }
            }
        }
    }

    if (
            pointState.depth() + 1 < m_structure.baseDepthEnd() &&
            pointState.depth() + 1 < m_depthEnd)
    {
        for (std::size_t i(0); i < dirHalfEnd(); ++i)
        {
            writeBase(pointState.getClimb(toDir(i)));
        }
    }
}

void Query::writeChunked()
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

            auto& extra(cr->extra(m_name, m_outSchema.filter("Mask")));

            while (it != range.end)
            {
                const PointInfo& info(*it);
                if (m_queryBounds.contains(info.point()))
                {
                    m_table.setPoint(info.data());
                    m_writeTable->setPoint(m_pos);
                    if (m_filter.check(m_pointRef))
                    {
                        if (!m_maskId || m_writeRef->getFieldAs<bool>(*m_maskId))
                        {
                            m_writeRef->getPackedData(
                                    extra.dimTypeList(),
                                    extra.get(info.offset()));
                        }
                        m_pos += m_outSchema.pointSize();
                        ++m_numPoints;
                    }
                }
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

bool Query::processPoint(
        std::vector<char>& buffer,
        const PointInfo& info,
        const std::size_t baseDepth)
{
    if (!m_queryBounds.contains(info.point())) return false;
    m_table.setPoint(info.data());
    if (!m_filter.check(m_pointRef)) return false;

    buffer.resize(buffer.size() + m_outSchema.pointSize(), 0);
    char* pos(buffer.data() + buffer.size() - m_outSchema.pointSize());

    std::size_t dimNum(0);
    const auto& mid(m_reader.metadata().boundsScaledCubic().mid());

    for (const auto& dim : m_outSchema.dims())
    {
        // Subtract one to ignore Dimension::Id::Unknown.
        dimNum = pdal::Utils::toNative(dim.id()) - 1;

        // Up to this point, everything has been in our local coordinate
        // system.  Query bounds were transformed to match our local view
        // of the world, as well as spatial attributes in the filter.  Now
        // that we've selected a point in our own local space, finally we
        // will transform that selection into user-requested space.
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
                    setAs<double>(pos, d);
                    break;
                case pdal::Dimension::Type::Float:
                    setAs<float>(pos, d);
                    break;
                case pdal::Dimension::Type::Unsigned8:
                    setAs<uint8_t>(pos, d);
                    break;
                case pdal::Dimension::Type::Signed8:
                    setAs<int8_t>(pos, d);
                    break;
                case pdal::Dimension::Type::Unsigned16:
                    setAs<uint16_t>(pos, d);
                    break;
                case pdal::Dimension::Type::Signed16:
                    setAs<int16_t>(pos, d);
                    break;
                case pdal::Dimension::Type::Unsigned32:
                    setAs<uint32_t>(pos, d);
                    break;
                case pdal::Dimension::Type::Signed32:
                    setAs<int32_t>(pos, d);
                    break;
                case pdal::Dimension::Type::Unsigned64:
                    setAs<uint64_t>(pos, d);
                    break;
                case pdal::Dimension::Type::Signed64:
                    setAs<int64_t>(pos, d);
                    break;
                default:
                    break;
            }
        }
        else if (m_reader.metadata().schema().contains(dim.name()))
        {
            m_pointRef.getField(pos, dim.id(), dim.type());
        }
        else if (baseDepth)
        {
            if (m_baseExtras.count(dim.name()))
            {
                static std::mutex m;
                std::lock_guard<std::mutex> lock(m);
                auto pr(m_baseExtra->table(baseDepth).at(info.offset()));
                pr.getField(pos, dim.id(), dim.type());
            }
        }
        else if (m_extras.count(dim.name()))
        {
            auto& extra(*m_extras.at(dim.name()));
            auto& table(extra.table());
            auto pr(table.at(info.offset()));
            pr.getField(pos, dim.id(), dim.type());
        }

        pos += dim.size();
    }

    return true;
}

} // namespace entwine


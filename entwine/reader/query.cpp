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
    std::size_t minPointsPerIteration(65536);
}

Delta Query::localize(const Delta& out) const
{
    const Delta in(m_reader.metadata().delta());
    return Delta(out.scale() / in.scale(), out.offset() - in.offset());
}

Bounds Query::localize(const Bounds& q, const Delta& localDelta) const
{
    const auto e(Bounds::everything());
    if (localDelta.empty() || q == e) return q;

    const Bounds indexedBounds(m_reader.metadata().boundsScaledCubic());

    const Point refCenter(
            Bounds(
                Point::scale(
                    indexedBounds.min(),
                    indexedBounds.mid(),
                    localDelta.scale(),
                    localDelta.offset()),
                Point::scale(
                    indexedBounds.max(),
                    indexedBounds.mid(),
                    localDelta.scale(),
                    localDelta.offset())).mid());

    const Bounds queryTransformed(
            Point::unscale(q.min(), Point(), localDelta.scale(), -refCenter),
            Point::unscale(q.max(), Point(), localDelta.scale(), -refCenter));

    Bounds queryCube(
            queryTransformed.min() + indexedBounds.mid(),
            queryTransformed.max() + indexedBounds.mid());

    // If the query bounds were 2d, make sure we maintain maximal extents.
    if (q.min().z == e.min().z && q.max().z == e.max().z)
    {
        queryCube = Bounds(
                Point(queryCube.min().x, queryCube.min().y, e.min().z),
                Point(queryCube.max().x, queryCube.max().y, e.max().z));
    }

    return queryCube;
}

Query::Query(
        const Reader& reader,
        const Bounds& bounds,
        const Delta& delta,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const Json::Value& filter)
    : m_reader(reader)
    , m_metadata(m_reader.metadata())
    , m_structure(m_metadata.structure())
    , m_delta(localize(delta))
    , m_bounds(localize(bounds, m_delta))
    , m_depthBegin(depthBegin)
    , m_depthEnd(depthEnd ? depthEnd : std::numeric_limits<uint32_t>::max())
    , m_filter(m_reader.metadata(), m_bounds, filter, &m_delta)
    , m_table(m_reader.metadata().schema())
    , m_pointRef(m_table, 0)
{
    if (!m_depthEnd || m_depthEnd > m_structure.coldDepthBegin())
    {
        QueryChunkState chunkState(m_structure, m_metadata.boundsScaledCubic());
        getFetches(chunkState);
    }
}

void Query::getFetches(const QueryChunkState& c)
{
    if (!m_filter.check(c.bounds())) return;

    if (c.depth() >= m_structure.coldDepthBegin())
    {
        if (!m_reader.exists(c)) return;
        if (c.depth() >= m_depthBegin)
        {
            m_chunks.emplace(m_reader, c.chunkId(), c.bounds(), c.depth());
        }
    }

    if (c.depth() + 1 < m_depthEnd)
    {
        if (c.allDirections())
        {
            for (std::size_t i(0); i < dirHalfEnd(); ++i)
            {
                getFetches(c.getClimb(toDir(i)));
            }
        }
        else getFetches(c.getClimb());
    }
}

bool Query::next()
{
    if (m_done) throw std::runtime_error("Called next after query completed");

    const std::size_t startPoints(m_numPoints);

    while (!m_done && m_numPoints - startPoints < minPointsPerIteration)
    {
        if (m_base)
        {
            m_base = false;

            if (m_reader.base())
            {
                PointState ps(m_structure, m_metadata.boundsScaledCubic());
                getBase(ps);
                m_done = m_chunks.empty();
            }
        }
        else getChunked();
    }

    return !m_done;
}

void Query::getBase(const PointState& pointState)
{
    if (!m_bounds.overlaps(pointState.bounds(), true)) return;

    if (pointState.depth() >= m_structure.baseDepthBegin())
    {
        const auto& cell(m_reader.base()->tubeData(pointState.index()));
        if (cell.empty()) return;

        if (pointState.depth() >= m_depthBegin)
        {
            for (const PointInfo& pointInfo : cell) processPoint(pointInfo);
        }
    }

    if (
            pointState.depth() + 1 < m_structure.baseDepthEnd() &&
            pointState.depth() + 1 < m_depthEnd)
    {
        for (std::size_t i(0); i < dirHalfEnd(); ++i)
        {
            getBase(pointState.getClimb(toDir(i)));
        }
    }
}

void Query::maybeAcquire()
{
    if (m_block || m_chunks.empty()) return;

    const auto begin(m_chunks.begin());
    auto end(m_chunks.begin());
    std::advance(end, std::min(fetchesPerIteration, m_chunks.size()));

    FetchInfoSet fetches(begin, end);
    m_block = m_reader.cache().acquire(m_reader.path(), fetches);
    m_chunks.erase(begin, end);

    if (m_block) m_chunkReaderIt = m_block->chunkMap().begin();
}

void Query::getChunked()
{
    maybeAcquire();

    if (m_block)
    {
        if (const ChunkReader* cr = m_chunkReaderIt->second)
        {
            ChunkReader::QueryRange range(cr->candidates(m_bounds));
            auto it(range.begin);

            while (it != range.end)
            {
                processPoint(*it);
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

void Query::processPoint(const PointInfo& info)
{
    if (!m_bounds.contains(info.point())) return;
    m_table.setPoint(info.data());
    if (!m_filter.check(m_pointRef)) return;
    process(info);
    ++m_numPoints;
}

void ReadQuery::process(const PointInfo& info)
{
    m_data.resize(m_data.size() + m_schema.pointSize(), 0);
    char* pos(m_data.data() + m_data.size() - m_schema.pointSize());

    std::size_t dimNum(0);

    for (const auto& dim : m_schema.dims())
    {
        dimNum = pdal::Utils::toNative(dim.id()) - 1;
        if (m_delta.exists() && dimNum < 3)
        {
            setScaled(dim, dimNum, pos);
        }
        else if (m_reader.metadata().schema().contains(dim.name()))
        {
            m_pointRef.getField(pos, dim.id(), dim.type());
        }
        /*
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
        */

        pos += dim.size();
    }
}

/*
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
            for (std::size_t i(0); i < dirHalfEnd(); ++i)
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
*/

} // namespace entwine


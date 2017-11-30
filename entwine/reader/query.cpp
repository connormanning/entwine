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
    if (!q.is3d())
    {
        queryCube = Bounds(
                Point(queryCube.min().x, queryCube.min().y, e.min().z),
                Point(queryCube.max().x, queryCube.max().y, e.max().z));
    }

    queryCube.shrink(indexedBounds);

    return queryCube;
}

Query::Query(const Reader& reader, const QueryParams& p)
    : m_reader(reader)
    , m_params(p)
    , m_metadata(m_reader.metadata())
    , m_structure(m_metadata.structure())
    , m_delta(
            p.nativeBounds() ?
                p.delta() :
                localize(p.delta()))
    , m_bounds(
            p.nativeBounds() ?
                localize(*p.nativeBounds(), m_metadata.delta()->inverse()) :
                localize(p.bounds(), m_delta))
    , m_depthBegin(p.db())
    , m_depthEnd(p.de() ? p.de() : std::numeric_limits<uint32_t>::max())
    , m_filter(m_reader.metadata(), m_bounds, p.filter(), &m_delta)
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
                if (m_depthBegin < m_structure.baseDepthEnd())
                {
                    chunk(m_reader.base()->chunk());
                }

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
        const auto& tube(m_reader.base()->tubeData(pointState.index()));
        if (tube.empty()) return;

        if (pointState.depth() >= m_depthBegin)
        {
            for (const PointInfo& pointInfo : tube) processPoint(pointInfo);
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
        if (const ColdChunkReader* cr = m_chunkReaderIt->second)
        {
            chunk(cr->chunk());

            ColdChunkReader::QueryRange range(cr->candidates(m_bounds));
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

RegisteredSchema::RegisteredSchema(const Reader& r, const Schema& output)
    : m_original(output)
{
    const Schema& native(r.metadata().schema());
    for (const auto& d : output.dims())
    {
        if (native.contains(d.name()))
        {
            m_dims.emplace_back(native, d);
        }
        else
        {
            const std::string name(r.findAppendName(d.name()));
            if (name.size()) m_dims.emplace_back(r.appendAt(name), d, false);
            else m_dims.emplace_back(native, d);
        }
    }
}

ReadQuery::ReadQuery(
        const Reader& reader,
        const QueryParams& params,
        const Schema& schema)
    : Query(reader, params)
    , m_schema(schema.empty() ? m_metadata.schema() : schema)
    , m_reg(reader, m_schema)
    , m_mid(
            params.nativeBounds() ?
                m_delta.offset() :
                m_metadata.boundsScaledCubic().mid())
{ }

void ReadQuery::chunk(const ChunkReader& cr)
{
    m_cr = &cr;
    for (auto& d : m_reg.dims())
    {
        if (!d.native())
        {
            const auto appendName(m_reader.findAppendName(d.info().name()));
            const Schema& appendSchema(m_reader.appendAt(appendName));
            d.setAppend(cr.findAppend(appendName, appendSchema));
        }
    }
}

void ReadQuery::process(const PointInfo& info)
{
    m_data.resize(m_data.size() + m_schema.pointSize(), 0);
    char* pos(m_data.data() + m_data.size() - m_schema.pointSize());

    std::size_t dimNum(0);

    for (const auto& dim : m_reg.dims())
    {
        const DimInfo& dimInfo(dim.info());
        dimNum = pdal::Utils::toNative(dimInfo.id()) - 1;
        if ((m_delta.exists() || m_params.nativeBounds()) && dimNum < 3)
        {
            setScaled(dimInfo, dimNum, pos);
        }
        else if (dim.native())
        {
            m_pointRef.getField(pos, dimInfo.id(), dimInfo.type());
        }
        else if (Append* append = dim.append())
        {
            auto pr(append->table().at(info.offset()));
            pr.getField(pos, dimInfo.id(), dimInfo.type());
        }

        pos += dimInfo.size();
    }
}

void WriteQuery::chunk(const ChunkReader& cr)
{
    m_append = &cr.getOrCreateAppend(m_name, m_schema);
}

void WriteQuery::process(const PointInfo& info)
{
    m_table.setPoint(m_pos);
    if (m_pr.getFieldAs<bool>(m_skipId)) return;
    m_append->insert(m_pr, info.offset());
}

} // namespace entwine


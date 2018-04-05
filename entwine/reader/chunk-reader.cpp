/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/reader/chunk-reader.hpp>

#include <pdal/PointRef.hpp>

#include <entwine/tree/chunk.hpp>
#include <entwine/types/binary-point-table.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/storage.hpp>
#include <entwine/util/compression.hpp>

namespace entwine
{

namespace
{
    const std::size_t poolBlockSize(1024);
}

ChunkReader::ChunkReader(
        const Metadata& metadata,
        const arbiter::Endpoint& endpoint,
        const arbiter::Endpoint& tmp,
        const Bounds& bounds,
        PointPool& pool,
        const Id& id,
        const std::size_t depth)
    : m_endpoint(endpoint)
    , m_metadata(metadata)
    , m_pool(pool.schema(), pool.delta(), poolBlockSize)
    , m_bounds(bounds)
    , m_schema(metadata.schema())
    , m_id(id)
    , m_depth(depth)
    , m_cells(metadata.storage().deserialize(endpoint, tmp, m_pool, m_id))
{ }

ChunkReader::ChunkReader(
        const Metadata& m,
        const arbiter::Endpoint& ep,
        const arbiter::Endpoint& tmp,
        PointPool& pool)
    : m_endpoint(ep)
    , m_metadata(m)
    , m_pool(pool.schema(), pool.delta(), poolBlockSize)
    , m_bounds(m.boundsScaledCubic())
    , m_schema(m.schema())
    , m_id(m.structure().baseIndexBegin())
    , m_depth(m.structure().baseDepthBegin())
    , m_cells(m_pool.cellPool())
{
    const Structure& s(m.structure());
    if (m.slicedBase())
    {
        for (std::size_t d(s.baseDepthBegin()); d < s.baseDepthBegin() + 3; ++d)
        {
            const auto id(ChunkInfo::calcLevelIndex(2, d));
            m_cells.pushBack(m.storage().deserialize(ep, tmp, m_pool, id));
            m_offsets.push_back(m_cells.size());
        }
    }
    else initLegacyBase(tmp);
}

void ChunkReader::initLegacyBase(const arbiter::Endpoint& tmp)
{
    const auto& m(m_metadata);

    const Schema tubeDim({ { "TubeId", "unsigned", 8 } });
    const Schema celledSchema(tubeDim.append(m.schema()));
    PointPool celledPool(celledSchema, m.delta());
    auto tubedCells(m.storage().deserialize(m_endpoint, tmp, celledPool, m_id));
    Data::PooledStack tubedData(celledPool.dataPool());

    const std::size_t numPoints(tubedCells.size());

    auto dataNodes(m_pool.dataPool().acquire(numPoints));
    m_cells = m_pool.cellPool().acquire(numPoints);

    const std::size_t celledPointSize(celledSchema.pointSize());
    const std::size_t tubeIdSize(sizeof(uint64_t));
    uint64_t tube(0);
    char* tPos(reinterpret_cast<char*>(&tube));

    BinaryPointTable table(m.schema());
    pdal::PointRef pointRef(table, 0);

    std::size_t slice(m.structure().baseDepthBegin());

    Cell::RawNode* cell(m_cells.head());
    Cell::RawNode* tubedCell(tubedCells.head());

    for (std::size_t i(0); i < numPoints; ++i)
    {
        const char* src((*tubedCell)->uniqueData());
        tubedData.push((*tubedCell)->acquire());

        Data::PooledNode data(dataNodes.popOne());

        std::copy(src, src + tubeIdSize, tPos);
        std::copy(src + tubeIdSize, src + celledPointSize, *data);

        table.setPoint(*data);
        (*cell)->set(pointRef, std::move(data));

        tube += m_id.getSimple();

        if (ChunkInfo::calcDepth(tube) > slice)
        {
            m_offsets.push_back(i);
            ++slice;
            if (ChunkInfo::calcDepth(tube) != slice)
            {
                throw std::runtime_error("Invalid legacy base");
            }
        }

        cell = cell->next();
        tubedCell = tubedCell->next();
    }
    m_offsets.push_back(numPoints);
}

ChunkReader::~ChunkReader()
{
    m_pool.release(std::move(m_cells));
    for (const auto& p : m_appends) p.second->write();
}

ColdChunkReader::ColdChunkReader(
        const Metadata& m,
        const arbiter::Endpoint& ep,
        const arbiter::Endpoint& tmp,
        const Bounds& bounds,
        PointPool& pool,
        const Id& id,
        std::size_t depth)
    : m_chunk(m, ep, tmp, bounds, pool, id, depth)
{
    m_points.reserve(m_chunk.cells().size());

    const auto& globalBounds(m.boundsScaledCubic());
    std::size_t offset(0);

    for (const auto& cell : m_chunk.cells())
    {
        m_points.emplace_back(
                offset,
                cell.point(),
                cell.uniqueData(),
                Tube::calcTick(cell.point(), globalBounds, depth));
        ++offset;
    }

    std::sort(m_points.begin(), m_points.end());
}

ColdChunkReader::QueryRange ColdChunkReader::candidates(const Bounds& qb) const
{
    if (qb.contains(m_chunk.bounds()))
    {
        return QueryRange(m_points.begin(), m_points.end());
    }

    const auto& gb(m_chunk.metadata().boundsScaledCubic());
    const PointInfo min(Tube::calcTick(qb.min(), gb, m_chunk.depth()));
    const PointInfo max(Tube::calcTick(qb.max(), gb, m_chunk.depth()));

    It begin(std::lower_bound(m_points.begin(), m_points.end(), min));
    It end(std::upper_bound(m_points.begin(), m_points.end(), max));

    return QueryRange(begin, end);
}

BaseChunkReader::BaseChunkReader(
        const Metadata& m,
        const arbiter::Endpoint& ep,
        const arbiter::Endpoint& tmp,
        PointPool& pool)
    : m_chunk(m, ep, tmp, pool)
{
    const Structure& s(m.structure());
    const auto& globalBounds(m.boundsScaledCubic());
    Climber climber(m);

    std::size_t offset(0);
    std::size_t slice(0);

    for (const auto& cell : m_chunk.cells())
    {
        const std::size_t depth(slice + s.baseDepthBegin());
        climber.reset();
        climber.magnifyTo(cell.point(), depth);
        m_points[climber.index()].emplace_back(
                offset,
                cell.point(),
                cell.uniqueData(),
                Tube::calcTick(cell.point(), globalBounds, depth));

        if (++offset == m_chunk.offsets().at(slice)) ++slice;
    }
}

} // namespace entwine


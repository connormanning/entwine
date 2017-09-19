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

ChunkReader::ChunkReader(
        const Metadata& metadata,
        const arbiter::Endpoint& endpoint,
        const Bounds& bounds,
        PointPool& pool,
        const Id& id,
        const std::size_t depth)
    : m_endpoint(endpoint)
    , m_metadata(metadata)
    , m_pool(pool)
    , m_bounds(bounds)
    , m_schema(metadata.schema())
    , m_id(id)
    , m_depth(depth)
    , m_cells(metadata.storage().deserialize(endpoint, pool, m_id))
{ }

ChunkReader::ChunkReader(
        const Metadata& m,
        const arbiter::Endpoint& ep,
        PointPool& pool)
    : m_endpoint(ep)
    , m_metadata(m)
    , m_pool(pool)
    , m_bounds(m.boundsScaledCubic())
    , m_schema(m.schema())
    , m_id(m.structure().baseIndexBegin())
    , m_depth(m.structure().baseDepthBegin())
    , m_cells(m_pool.cellPool())
{
    const Structure& s(m.structure());
    for (std::size_t d(s.baseDepthBegin()); d < s.baseDepthBegin() + 3; ++d)
    {
        const auto id(ChunkInfo::calcLevelIndex(2, d));
        m_cells.pushBack(m.storage().deserialize(ep, pool, id));
        m_offsets.push_back(m_cells.size());
    }
}

ChunkReader::~ChunkReader()
{
    m_pool.release(std::move(m_cells));
    for (const auto& p : m_appends) p.second->write();
}

ColdChunkReader::ColdChunkReader(
        const Metadata& m,
        const arbiter::Endpoint& ep,
        const Bounds& bounds,
        PointPool& pool,
        const Id& id,
        std::size_t depth)
    : m_chunk(m, ep, bounds, pool, id, depth)
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
        PointPool& pool)
    : m_chunk(m, ep, pool)
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
        if (++offset == m_chunk.offsets().at(slice))
        {
            ++slice;
        }
    }
}













/*
BetterBaseChunkReader::BetterBaseChunkReader(
        const Metadata& m,
        const arbiter::Endpoint& ep,
        PointPool& pool)
    : m_ep(ep)
    , m_id(m.structure().baseIndexBegin())
{
    const Structure& s(m.structure());
    const auto b(m.boundsScaledCubic());
    for (std::size_t d(s.baseDepthBegin()); d < s.baseDepthEnd(); ++d)
    {
        const auto id(ChunkInfo::calcLevelIndex(2, d));
        m_slices.push_back(makeUnique<BChunkReader>(m, m_ep, b, pool, id, d));
    }
}

BaseChunkReader::BaseChunkReader(
        const Metadata& m,
        const arbiter::Endpoint& ep,
        PointPool& pool)
    : m_ep(ep)
    , m_id(m.structure().baseIndexBegin())
    , m_pool(pool)
    , m_cells(m_pool.cellPool())
    , m_points(m.structure().baseIndexSpan())
{ }

BaseChunkReader::~BaseChunkReader()
{
    m_pool.release(std::move(m_cells));
    // for (auto& e : m_extras) e.second.write();
}

SlicedBaseChunkReader::SlicedBaseChunkReader(
        const Metadata& m,
        PointPool& pool,
        const arbiter::Endpoint& endpoint)
    : BaseChunkReader(m, endpoint, pool)
{
    const Structure& s(m.structure());
    Pool t(s.baseDepthEnd() - s.baseDepthBegin());
    std::mutex mutex;

    m_slices.resize(s.baseDepthEnd() - s.baseDepthBegin());
    m_baseDepthBegin = s.baseDepthBegin();

    for (std::size_t d(s.baseDepthBegin()); d < s.baseDepthEnd(); ++d)
    {
        // t.add([this, &s, &mutex, &m, &endpoint, d]()
        // {
            const auto id(ChunkInfo::calcLevelIndex(2, d));
            auto cells(m.storage().deserialize(endpoint, m_pool, id));
            Climber climber(m);

            m_slices.at(d - s.baseDepthBegin()) = cells.size();

            std::lock_guard<std::mutex> lock(mutex);

            std::size_t offset(0);
            for (const auto& cell : cells)
            {
                climber.reset();
                climber.magnifyTo(cell.point(), d);
                m_points.at(normalize(climber.index())).emplace_back(
                        offset,
                        cell.point(),
                        cell.uniqueData());
                ++offset;
            }

            m_cells.push(std::move(cells));
        // });
    }

    t.join();
}

CelledBaseChunkReader::CelledBaseChunkReader(
        const Metadata& m,
        PointPool& pool,
        const arbiter::Endpoint& endpoint)
    : BaseChunkReader(m, endpoint, pool)
{
    DimList dims;
    dims.push_back(DimInfo("TubeId", "unsigned", 8));
    dims.insert(dims.end(), m.schema().dims().begin(), m.schema().dims().end());
    const Schema celledSchema(dims);
    PointPool celledPool(celledSchema, m.delta());

    auto tubedCells(m.storage().deserialize(endpoint, celledPool, m_id));
    Data::PooledStack tubedData(celledPool.dataPool());

    auto dataNodes(m_pool.dataPool().acquire(tubedCells.size()));
    m_cells = m_pool.cellPool().acquire(tubedCells.size());

    const std::size_t celledPointSize(celledSchema.pointSize());
    const std::size_t tubeIdSize(sizeof(uint64_t));
    uint64_t tube(0);
    char* tPos(reinterpret_cast<char*>(&tube));

    BinaryPointTable table(m.schema());
    pdal::PointRef pointRef(table, 0);

    std::size_t offset(0);
    for (auto& cell : m_cells)
    {
        auto tubedCell(tubedCells.popOne());
        const char* src(tubedCell->uniqueData());

        Data::PooledNode data(dataNodes.popOne());

        std::copy(src, src + tubeIdSize, tPos);
        std::copy(src + tubeIdSize, src + celledPointSize, *data);

        table.setPoint(*data);
        cell.set(pointRef, std::move(data));

        m_points.at(tube).emplace_back(offset, cell.point(), cell.uniqueData());
        ++offset;

        tubedData.push(tubedCell->acquire());
    }
}
*/

} // namespace entwine


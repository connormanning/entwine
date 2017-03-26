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

#include <entwine/third/arbiter/arbiter.hpp>
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
        PointPool& pool,
        const Id& id,
        const std::size_t depth)
    : m_pool(pool)
    , m_schema(metadata.schema())
    , m_bounds(metadata.boundsScaledCubic())
    , m_id(id)
    , m_depth(depth)
    , m_cells(metadata.storage().deserialize(endpoint, pool, m_id))
{
    const std::size_t numPoints(m_cells.size());
    m_points.reserve(numPoints);

    for (const auto& cell : m_cells)
    {
        m_points.emplace_back(
                cell.point(),
                cell.uniqueData(),
                Tube::calcTick(cell.point(), m_bounds, m_depth));
    }

    std::sort(m_points.begin(), m_points.end());
}

ChunkReader::~ChunkReader()
{
    m_pool.release(std::move(m_cells));
}

ChunkReader::QueryRange ChunkReader::candidates(const Bounds& queryBounds) const
{
    const PointInfo min(Tube::calcTick(queryBounds.min(), m_bounds, m_depth));
    const PointInfo max(Tube::calcTick(queryBounds.max(), m_bounds, m_depth));

    It begin(std::lower_bound(m_points.begin(), m_points.end(), min));
    It end(std::upper_bound(m_points.begin(), m_points.end(), max));

    return QueryRange(begin, end);
}

BaseChunkReader::BaseChunkReader(const Metadata& m, PointPool& pool)
    : m_id(m.structure().baseIndexBegin())
    , m_pool(pool)
    , m_cells(m_pool.cellPool())
    , m_points(m.structure().baseIndexSpan())
{ }

BaseChunkReader::~BaseChunkReader() { m_pool.release(std::move(m_cells)); }

SlicedBaseChunkReader::SlicedBaseChunkReader(
        const Metadata& m,
        PointPool& pool,
        const arbiter::Endpoint& endpoint)
    : BaseChunkReader(m, pool)
{
    const Structure& s(m.structure());
    Pool t(s.baseDepthEnd() - s.baseDepthBegin());
    std::mutex mutex;

    for (std::size_t d(s.baseDepthBegin()); d < s.baseDepthEnd(); ++d)
    {
        t.add([this, &mutex, &m, &endpoint, d]()
        {
            const auto id(ChunkInfo::calcLevelIndex(2, d));
            auto cells(m.storage().deserialize(endpoint, m_pool, id));
            Climber climber(m);

            std::lock_guard<std::mutex> lock(mutex);

            for (const auto& cell : cells)
            {
                climber.reset();
                climber.magnifyTo(cell.point(), d);
                m_points.at(normalize(climber.index())).emplace_back(
                        cell.point(),
                        cell.uniqueData());
            }

            m_cells.push(std::move(cells));
        });
    }

    t.join();
}

CelledBaseChunkReader::CelledBaseChunkReader(
        const Metadata& m,
        PointPool& pool,
        const arbiter::Endpoint& endpoint)
    : BaseChunkReader(m, pool)
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

    for (auto& cell : m_cells)
    {
        auto tubedCell(tubedCells.popOne());
        const char* src(tubedCell->uniqueData());

        Data::PooledNode data(dataNodes.popOne());

        std::copy(src, src + tubeIdSize, tPos);
        std::copy(src + tubeIdSize, src + celledPointSize, *data);

        table.setPoint(*data);
        cell.set(pointRef, std::move(data));

        m_points.at(tube).emplace_back(cell.point(), cell.uniqueData());

        tubedData.push(tubedCell->acquire());
    }
}

} // namespace entwine


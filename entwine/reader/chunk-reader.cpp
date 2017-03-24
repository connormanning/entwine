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
#include <entwine/types/format.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/compression.hpp>

namespace entwine
{

ChunkReader::ChunkReader(
        const Metadata& metadata,
        const arbiter::Endpoint& endpoint,
        PointPool& pool,
        const Id& id,
        const std::size_t depth)
    : m_schema(metadata.schema())
    , m_bounds(metadata.boundsScaledCubic())
    , m_id(id)
    , m_depth(depth)
    , m_cells(metadata.format().deserialize(endpoint, pool, m_id))
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

ChunkReader::QueryRange ChunkReader::candidates(const Bounds& queryBounds) const
{
    const PointInfo min(Tube::calcTick(queryBounds.min(), m_bounds, m_depth));
    const PointInfo max(Tube::calcTick(queryBounds.max(), m_bounds, m_depth));

    It begin(std::lower_bound(m_points.begin(), m_points.end(), min));
    It end(std::upper_bound(m_points.begin(), m_points.end(), max));

    return QueryRange(begin, end);
}

BaseChunkReader::BaseChunkReader(
        const Metadata& metadata,
        const arbiter::Endpoint& endpoint)
    : m_celledSchema(Schema::makeCelled(metadata.schema()))
    , m_pool(m_celledSchema, metadata.delta())
    , m_id(metadata.structure().baseIndexBegin())
    , m_cells(metadata.format().deserialize(endpoint, m_pool, m_id))
    , m_tubes(metadata.structure().baseIndexSpan())
{
    const std::size_t celledPointSize(m_celledSchema.pointSize());
    const std::size_t tubeIdSize(sizeof(uint64_t));
    const std::size_t tubeOffset(celledPointSize - tubeIdSize);
    uint64_t tube(0);
    char* tPos(reinterpret_cast<char*>(&tube));

    for (const auto& cell : m_cells)
    {
        const char* pos(cell.uniqueData());
        std::copy(pos + tubeOffset, pos + celledPointSize, tPos);
        m_tubes.at(tube).emplace_back(cell.point(), pos);
    }
}

} // namespace entwine


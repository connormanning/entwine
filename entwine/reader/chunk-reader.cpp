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
#include <entwine/types/metadata.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/compression.hpp>

namespace entwine
{

ChunkReader::ChunkReader(
        const Schema& schema,
        const Bounds& bounds,
        const Id& id,
        const std::size_t depth,
        std::unique_ptr<std::vector<char>> compressed)
    : m_schema(schema)
    , m_bounds(bounds)
    , m_id(id)
    , m_depth(depth)
    , m_data()
    , m_points()
{
    const std::size_t numPoints(Chunk::popTail(*compressed).numPoints);
    m_data = Compression::decompress(*compressed, m_schema, numPoints);

    BinaryPointTable table(m_schema);
    pdal::PointRef pointRef(table, 0);

    const std::size_t pointSize(m_schema.pointSize());
    const char* pos(m_data->data());
    Point point;

    for (std::size_t i(0); i < numPoints; ++i)
    {
        table.setPoint(pos);

        point.x = pointRef.getFieldAs<double>(pdal::Dimension::Id::X);
        point.y = pointRef.getFieldAs<double>(pdal::Dimension::Id::Y);
        point.z = pointRef.getFieldAs<double>(pdal::Dimension::Id::Z);

        m_points.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(Tube::calcTick(point, m_bounds, m_depth)),
                std::forward_as_tuple(point, pos));

        pos += pointSize;
    }
}

ChunkReader::QueryRange ChunkReader::candidates(const Bounds& queryBounds) const
{
    const std::size_t minTick(
            Tube::calcTick(queryBounds.min(), m_bounds, m_depth));

    const std::size_t maxTick(
            Tube::calcTick(queryBounds.max(), m_bounds, m_depth));

    It begin(m_points.lower_bound(minTick));
    It end(m_points.upper_bound(maxTick));

    return QueryRange(begin, end);
}

BaseChunkReader::BaseChunkReader(
        const Metadata& metadata,
        const Schema& celledSchema,
        const Id& id,
        std::unique_ptr<std::vector<char>> compressed)
    : m_id(id)
    , m_data()
    , m_tubes(metadata.structure().baseIndexSpan())
{
    const std::size_t numPoints(Chunk::popTail(*compressed).numPoints);
    m_data = Compression::decompress(*compressed, celledSchema, numPoints);

    BinaryPointTable table(celledSchema);
    pdal::PointRef pointRef(table, 0);

    const std::size_t pointSize(celledSchema.pointSize());
    const char* pos(m_data->data());

    uint64_t tube(0);
    Point point;
    const auto tubeId(celledSchema.getId("TubeId"));
    const std::size_t dataOffset(sizeof(uint64_t));

    for (std::size_t i(0); i < numPoints; ++i)
    {
        table.setPoint(pos);

        tube = pointRef.getFieldAs<uint64_t>(tubeId);

        point.x = pointRef.getFieldAs<double>(pdal::Dimension::Id::X);
        point.y = pointRef.getFieldAs<double>(pdal::Dimension::Id::Y);
        point.z = pointRef.getFieldAs<double>(pdal::Dimension::Id::Z);

        m_tubes.at(tube).emplace_back(point, pos + dataOffset);

        pos += pointSize;
    }
}

} // namespace entwine


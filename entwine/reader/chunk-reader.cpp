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

#include <entwine/tree/chunk.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/compression.hpp>

namespace entwine
{

ChunkReader::ChunkReader(
        const Schema& schema,
        const BBox& bbox,
        const Id& id,
        const std::size_t depth,
        std::unique_ptr<std::vector<char>> compressed)
    : m_schema(schema)
    , m_bbox(bbox)
    , m_id(id)
    , m_depth(depth)
    , m_numPoints(Chunk::popTail(*compressed).numPoints)
    , m_data()
    , m_points()
{
    m_data = Compression::decompress(*compressed, m_schema, m_numPoints);

    BinaryPointTable table(m_schema);
    pdal::PointRef pointRef(table, 0);

    const std::size_t pointSize(m_schema.pointSize());
    char* pos(m_data->data());
    Point point;

    for (std::size_t i(0); i < m_numPoints; ++i)
    {
        table.setPoint(pos);

        point.x = pointRef.getFieldAs<double>(pdal::Dimension::Id::X);
        point.y = pointRef.getFieldAs<double>(pdal::Dimension::Id::Y);
        point.z = pointRef.getFieldAs<double>(pdal::Dimension::Id::Z);

        m_points.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(Tube::calcTick(point, m_bbox, m_depth)),
                std::forward_as_tuple(point, pos));

        pos += pointSize;
    }
}

ChunkReader::QueryRange ChunkReader::candidates(const BBox& qbox) const
{
    const std::size_t minTick(Tube::calcTick(qbox.min(), m_bbox, m_depth));
    const std::size_t maxTick(Tube::calcTick(qbox.max(), m_bbox, m_depth));

    It begin(m_points.lower_bound(minTick));
    It end(m_points.upper_bound(maxTick));

    return QueryRange(begin, end);
}

} // namespace entwine


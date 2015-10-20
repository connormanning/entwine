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

#include <entwine/compression/util.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/single-point-table.hpp>

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
    const std::size_t pointSize(m_schema.pointSize());

    m_data = Compression::decompress(
                *compressed,
                schema,
                m_numPoints * pointSize);

    SinglePointTable table(m_schema);
    LinkingPointView view(table);

    Point point;
    const char* pos(m_data->data());

    for (std::size_t i(0); i < m_numPoints; ++i)
    {
        table.setData(pos);

        point.x = view.getFieldAs<double>(pdal::Dimension::Id::X, 0);
        point.y = view.getFieldAs<double>(pdal::Dimension::Id::Y, 0);
        point.z = view.getFieldAs<double>(pdal::Dimension::Id::Z, 0);

        m_points.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(
                    Tube::calcTick(
                        point,
                        m_bbox,
                        m_depth)),
                std::forward_as_tuple(point, pos));

        pos += pointSize;
    }
}

std::size_t ChunkReader::query(
        std::vector<char>& buffer,
        const Schema& outSchema,
        const BBox& qbox) const
{
    std::size_t numPoints(0);
    const std::size_t queryPointSize(outSchema.pointSize());
    char* pos(0);

    SinglePointTable table(m_schema);
    LinkingPointView view(table);

    const std::size_t minTick(Tube::calcTick(qbox.min(), m_bbox, m_depth));
    const std::size_t maxTick(Tube::calcTick(qbox.max(), m_bbox, m_depth));

    auto it(m_points.lower_bound(minTick));
    const auto end(m_points.upper_bound(maxTick));

    while (it != end)
    {
        const PointInfoNonPooled& info(it->second);
        const Point& point(info.point());
        if (qbox.contains(point))
        {
            ++numPoints;
            buffer.resize(buffer.size() + queryPointSize, 0);
            pos = buffer.data() + buffer.size() - queryPointSize;

            table.setData(info.data());

            for (const auto dim : outSchema.dims())
            {
                view.getField(pos, dim.id(), dim.type(), 0);
                pos += dim.size();
            }
        }

        ++it;
    }

    return numPoints;
}

} // namespace entwine


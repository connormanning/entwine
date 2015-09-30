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
        const Id& id,
        std::unique_ptr<std::vector<char>> compressed)
    : m_schema(schema)
    , m_id(id)
    , m_numPoints(Chunk::popTail(*compressed).numPoints)
    , m_data()
    , m_points()
{
    const std::size_t nativePointSize(m_schema.pointSize());
    const Schema celledSchema(Chunk::makeCelled(m_schema));
    const std::size_t celledPointSize(celledSchema.pointSize());

    auto celledData(
            Compression::decompress(
                *compressed,
                celledSchema,
                m_numPoints * celledPointSize));

    m_data.resize(nativePointSize * m_numPoints);

    SinglePointTable table(celledSchema);
    LinkingPointView view(table);

    const pdal::Dimension::Id::Enum cellId(
            celledSchema.pdalLayout().findDim("CellId"));

    char* pos(celledData->data());
    char* out(m_data.data());

    const std::size_t nativeOffset(2 * sizeof(uint64_t));

    for (std::size_t i(0); i < m_numPoints; ++i)
    {
        table.setData(pos);
        std::copy(pos + nativeOffset, pos + celledPointSize, out);

        m_points.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(view.getFieldAs<uint64_t>(cellId, 0)),
                std::forward_as_tuple(
                    Point(
                        view.getFieldAs<double>(pdal::Dimension::Id::X, 0),
                        view.getFieldAs<double>(pdal::Dimension::Id::Y, 0),
                        view.getFieldAs<double>(pdal::Dimension::Id::Z, 0)),
                    out));

        pos += celledPointSize;
        out += nativePointSize;
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

    for (const auto& entry : m_points)
    {
        const PointInfoShallow& info(entry.second);
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
        else std::cout << "NO!" << std::endl;
    }

    return numPoints;
}

} // namespace entwine


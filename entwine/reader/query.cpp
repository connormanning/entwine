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

#include <entwine/types/schema.hpp>

namespace entwine
{

Query::Query(Reader& reader, const Schema& outSchema, ChunkMap chunkMap)
    : m_reader(reader)
    , m_outSchema(outSchema)
    , m_chunkMap(chunkMap)
    , m_points()
    , m_table(m_reader.schema(), 0)
    , m_view(m_table)
{ }

void Query::insert(const char* pos)
{
    m_points.push_back(pos);
}

Point Query::unwrapPoint(const char* pos) const
{
    m_table.setData(pos);

    return Point(
            m_view.getFieldAs<double>(pdal::Dimension::Id::X, 0),
            m_view.getFieldAs<double>(pdal::Dimension::Id::Y, 0),
            m_view.getFieldAs<double>(pdal::Dimension::Id::Z, 0));
}

std::size_t Query::size() const
{
    return m_points.size();
}

void Query::get(const std::size_t index, char* out) const
{
    m_table.setData(m_points.at(index));

    for (const auto& dim : m_outSchema.dims())
    {
        m_view.getField(out, dim.id(), dim.type(), 0);
        out += dim.size();
    }
}

std::vector<char> Query::get(const std::size_t index) const
{
    std::vector<char> data(m_outSchema.pointSize());
    get(index, data.data());
    return data;
}

} // namespace entwine


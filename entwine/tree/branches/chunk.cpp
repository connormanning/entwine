/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/branches/chunk.hpp>

#include <pdal/Dimension.hpp>
#include <pdal/PointView.hpp>

#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-table.hpp>
#include <entwine/types/single-point-table.hpp>

namespace entwine
{

Entry::Entry(std::atomic<const Point*>& point, char* pos, std::mutex& mutex)
    : point(point)
    , pos(pos)
    , mutex(mutex)
{ }

bool Entry::hasPoint() const
{
    return point.load() != 0;
}

Chunk::Chunk(
        const Schema& schema,
        const std::size_t id,
        const std::size_t numPoints)
    : m_schema(schema)
    , m_id(id)
    , m_numPoints(numPoints)
    , m_points(m_numPoints, std::atomic<const Point*>(0))
    , m_data()
    , m_locks(m_numPoints)
{
    SimplePointTable table(schema);
    pdal::PointView view(table);

    const auto emptyCoord(Point::emptyCoord());

    for (std::size_t i(0); i < numPoints; ++i)
    {
        view.setField(pdal::Dimension::Id::X, i, emptyCoord);
        view.setField(pdal::Dimension::Id::Y, i, emptyCoord);
    }

    m_data = table.data();
}

Chunk::Chunk(
        const Schema& schema,
        const std::size_t id,
        const std::vector<char>& data)
    : m_schema(schema)
    , m_id(id)
    , m_numPoints(data.size() / m_schema.pointSize())
    , m_points(m_numPoints, std::atomic<const Point*>(0))
    , m_data(std::move(data))
    , m_locks(m_numPoints)
{
    double x(0);
    double y(0);

    const std::size_t pointSize(m_schema.pointSize());

    if (data.size() != pointSize * m_numPoints)
    {
        throw std::runtime_error("Mismatched chunk size");
    }

    for (std::size_t i(0); i < m_numPoints; ++i)
    {
        char* pos(m_data.data() + i * pointSize);

        SinglePointTable table(m_schema, pos);
        LinkingPointView view(table);

        x = view.getFieldAs<double>(pdal::Dimension::Id::X, 0);
        y = view.getFieldAs<double>(pdal::Dimension::Id::Y, 0);

        if (Point::exists(x, y))
        {
            m_points[i].atom.store(new Point(x, y));
        }
    }
}

Chunk::~Chunk()
{
    for (auto& p : m_points)
    {
        if (p.atom.load()) delete p.atom.load();
    }
}

const std::vector<char>& Chunk::data() const
{
    return m_data;
}

std::unique_ptr<Entry> Chunk::getEntry(std::size_t rawIndex)
{
    const std::size_t index(normalizeIndex(rawIndex));

    return std::unique_ptr<Entry>(
            new Entry(
                m_points.at(index).atom,
                getPosition(index),
                m_locks[index]));
}

std::size_t Chunk::normalizeIndex(const std::size_t rawIndex) const
{
    assert(rawIndex >= m_id);
    return rawIndex - m_id;
}

char* Chunk::getPosition(std::size_t index)
{
    return m_data.data() + index * m_schema.pointSize();
}

} // namespace entwine


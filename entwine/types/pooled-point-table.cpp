/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/pooled-point-table.hpp>

#include <entwine/types/binary-point-table.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

std::unique_ptr<PooledPointTable> PooledPointTable::create(
        PointPool& pointPool,
        Process process,
        const Delta* delta,
        const Origin origin)
{
    if (pointPool.schema().normal())
    {
        return makeUnique<NormalPooledPointTable>(pointPool, process, origin);
    }
    else
    {
        return makeUnique<ConvertingPooledPointTable>(
                pointPool,
                makeUnique<Schema>(Schema::normalize(pointPool.schema())),
                process,
                *delta,
                origin);
    }
}

void PooledPointTable::reset()
{
    preprocess();

    BinaryPointTable table(outSchema());
    pdal::PointRef pointRef(table, 0);

    assert(m_cellNodes.size() >= outstanding());
    Cell::PooledStack cells(m_cellNodes.pop(outstanding()));

    for (auto& cell : cells)
    {
        auto data(m_dataNodes.popOne());
        table.setPoint(*data);

        if (m_origin != invalidOrigin)
        {
            pointRef.setField(pdal::Dimension::Id::OriginId, m_origin);
            pointRef.setField(pdal::Dimension::Id::PointId, m_index);
            ++m_index;
        }

        cell.set(pointRef, std::move(data));
    }

    cells = m_process(std::move(cells));
    for (auto& cell : cells) m_dataNodes.push(cell.acquire());
    m_cellNodes.push(std::move(cells));

    // Can't put this in allocate since allocate is called from the ctor.
    allocate();
    allocated();
}

void PooledPointTable::allocate()
{
    assert(m_dataNodes.size() == m_cellNodes.size());
    const std::size_t needs(capacity() - m_dataNodes.size());
    if (!needs) return;

    m_dataNodes.push(m_pointPool.dataPool().acquire(needs));
    m_cellNodes.push(m_pointPool.cellPool().acquire(needs));
}

void NormalPooledPointTable::allocated()
{
    m_refs.clear();
    for (char*& d : m_dataNodes) m_refs.push_back(d);
}

void ConvertingPooledPointTable::preprocess()
{
    BinaryPointTable preTable(*m_preSchema);
    pdal::PointRef prePointRef(preTable, 0);
    const std::size_t prePointSize(m_preSchema->pointSize());
    const std::size_t preOffset(
            m_preSchema->find("X").size() +
            m_preSchema->find("Y").size() +
            m_preSchema->find("Z").size());

    BinaryPointTable postTable(outSchema());
    pdal::PointRef postPointRef(postTable, 0);
    const std::size_t postOffset(
            outSchema().find("X").size() +
            outSchema().find("Y").size() +
            outSchema().find("Z").size());

    const Data::RawNode* node(m_dataNodes.head());
    const char* pos(m_preData.data());

    static const auto x(pdal::Dimension::Id::X);
    static const auto y(pdal::Dimension::Id::Y);
    static const auto z(pdal::Dimension::Id::Z);

    if (m_dataNodes.size() < outstanding())
    {
        std::cout << m_dataNodes.size() << " < " << outstanding() << std::endl;
        throw std::runtime_error("Bad data stack size");
    }

    Point p;

    for (std::size_t i(0); i < outstanding(); ++i)
    {
        preTable.setPoint(pos);
        postTable.setPoint(**node);

        p.x = prePointRef.getFieldAs<double>(x);
        p.y = prePointRef.getFieldAs<double>(y);
        p.z = prePointRef.getFieldAs<double>(z);

        p = Point::round(Point::scale(p, m_delta.scale(), m_delta.offset()));

        postPointRef.setField(x, p.x);
        postPointRef.setField(y, p.y);
        postPointRef.setField(z, p.z);

        std::copy(pos + preOffset, pos + prePointSize, **node + postOffset);

        node = node->next();
        pos += prePointSize;
    }
}

} // namespace entwine


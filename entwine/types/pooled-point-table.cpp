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
    if (!delta)
    {
        return makeUnique<PooledPointTable>(pointPool, process, origin);
    }
    else
    {
        return makeUnique<ConvertingPointTable>(
                pointPool,
                process,
                origin,
                *delta,
                makeUnique<Schema>(Schema::normalize(pointPool.schema())));
    }
}

void PooledPointTable::reset()
{
    BinaryPointTable table(m_schema);
    pdal::PointRef pointRef(table, 0);

    assert(m_cellNodes.size() >= outstanding());
    Cell::PooledStack cells(m_cellNodes.pop(outstanding()));

    for (auto& cell : cells)
    {
        auto data(m_dataNodes.popOne());
        table.setPoint(*data);

        if (m_origin != invalidOrigin)
        {
            pointRef.setField(pdal::Dimension::Id::PointId, m_index);
            pointRef.setField(pdal::Dimension::Id::OriginId, m_origin);
            ++m_index;
        }

        cell.set(pointRef, std::move(data));
    }

    cells = m_process(std::move(cells));
    for (auto& cell : cells) m_dataNodes.push(cell.acquire());
    m_cellNodes.push(std::move(cells));

    allocate();
}

void PooledPointTable::allocate()
{
    assert(m_dataNodes.size() == m_cellNodes.size());
    const std::size_t needs(capacity() - m_dataNodes.size());
    if (!needs) return;

    m_dataNodes.push(m_pointPool.dataPool().acquire(needs));
    m_cellNodes.push(m_pointPool.cellPool().acquire(needs));

    m_refs.clear();
    for (char*& d : m_dataNodes) m_refs.push_back(d);
}

} // namespace entwine


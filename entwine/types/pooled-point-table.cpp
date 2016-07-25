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

namespace entwine
{

namespace
{
    const std::size_t blockSize(4096);
}

PooledPointTable::PooledPointTable(
        PointPool& pointPool,
        std::function<Cell::PooledStack(Cell::PooledStack)> process,
        pdal::Dimension::Id originId,
        Origin origin)
    : pdal::StreamPointTable(pointPool.schema().pdalLayout())
    , m_pointPool(pointPool)
    , m_dataNodes(m_pointPool.dataPool())
    , m_refs(blockSize, nullptr)
    , m_size(0)
    , m_process(process)
    , m_originId(originId)
    , m_origin(origin)
{
    allocate();
}

pdal::point_count_t PooledPointTable::capacity() const
{
    return blockSize;
}

void PooledPointTable::reset()
{
    pdal::PointRef pointRef(*this, 0);

    // Using the pointRef during the loop ends up calling into this->getPoint,
    // which will hammer over our m_size as we're traversing - so store a copy.
    const std::size_t fixedSize(m_size);
    Cell::PooledStack cells(m_pointPool.cellPool().acquire(fixedSize));
    Cell::RawNode* cell(cells.head());

    for (std::size_t i(0); i < fixedSize; ++i)
    {
        pointRef.setPointId(i);
        (*cell)->set(pointRef, m_dataNodes.popOne());
        if (m_origin != invalidOrigin) pointRef.setField(m_originId, m_origin);

        cell = cell->next();
    }

    m_process(std::move(cells));
    m_size = 0;

    allocate();
}

void PooledPointTable::allocate()
{
    const std::size_t needs(blockSize - m_dataNodes.size());
    m_dataNodes.push(m_pointPool.dataPool().acquire(needs));

    Data::RawNode* node(m_dataNodes.head());
    for (std::size_t i(0); i < blockSize; ++i)
    {
        m_refs[i] = node;
        node = node->next();
    }
}

} // namespace entwine


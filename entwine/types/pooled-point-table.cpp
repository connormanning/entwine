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
        Pools& pools,
        std::function<PooledInfoStack(PooledInfoStack)> process,
        const pdal::Dimension::Id::Enum originId,
        const Origin origin)
    : pdal::StreamPointTable(pools.schema().pdalLayout())
    , m_pools(pools)
    , m_stack(pools.infoPool())
    , m_nodes(blockSize, nullptr)
    , m_size(0)
    , m_process(process)
    , m_originId(originId)
    , m_origin(origin)
{
    allocate();
}

PooledPointTable::PooledPointTable(
        Pools& pools,
        std::function<PooledInfoStack(PooledInfoStack)> process)
    : pdal::StreamPointTable(pools.schema().pdalLayout())
    , m_pools(pools)
    , m_stack(pools.infoPool())
    , m_nodes(blockSize, nullptr)
    , m_size(0)
    , m_process(process)
    , m_originId(pdal::Dimension::Id::Unknown)
    , m_origin(invalidOrigin)
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

    for (std::size_t i(0); i < fixedSize; ++i)
    {
        pointRef.setPointId(i);
        m_nodes[i]->val().point(pointRef);
        if (m_origin != invalidOrigin) pointRef.setField(m_originId, m_origin);
    }

    m_stack.push(m_process(m_stack.pop(fixedSize)));
    m_size = 0;

    allocate();
}

void PooledPointTable::allocate()
{
    const std::size_t needs(blockSize - m_stack.size());

    PooledInfoStack infoStack(m_pools.infoPool().acquire(needs));
    PooledDataStack dataStack(m_pools.dataPool().acquire(needs));

    RawInfoNode* info(infoStack.head());

    for (std::size_t i(0); i < needs; ++i)
    {
        info->construct(dataStack.popOne());
        info = info->next();
    }

    m_stack.push(std::move(infoStack));
    info = m_stack.head();

    for (std::size_t i(0); i < blockSize; ++i)
    {
        m_nodes[i] = info;
        info = info->next();
    }
}

} // namespace entwine


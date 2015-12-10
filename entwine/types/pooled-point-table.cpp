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
    const std::size_t chunkPoints(65536);
}

PooledPointTable::PooledPointTable(
        DataPool& dataPool,
        const Schema& schema,
        std::function<void(PooledDataStack)> process)
    : pdal::StreamPointTable(schema.pdalLayout())
    , m_dataPool(dataPool)
    , m_stack(dataPool)
    , m_nodes()
    , m_process(process)
    , m_size(0)
{ }

pdal::point_count_t PooledPointTable::capacity() const
{
    return chunkPoints;
}

void PooledPointTable::reset()
{
    m_nodes.erase(m_nodes.begin(), m_nodes.begin() + m_size);
    m_process(m_stack.pop(m_size));
    m_size = 0;
}

char* PooledPointTable::getPoint(const pdal::PointId i)
{
    assert(i <= m_stack.size());

    if (m_size == m_stack.size())
    {
        PooledDataStack newStack(m_dataPool.acquire(blockSize));
        RawDataNode* node(newStack.head());

        assert(newStack.size() == m_blockSize);

        const std::size_t startSize(m_nodes.size());
        m_nodes.resize(startSize + blockSize);

        for (std::size_t i(startSize); i < startSize + blockSize; ++i)
        {
            m_nodes[i] = node;
            node = node->next();
        }

        newStack.push(std::move(m_stack));
        m_stack = std::move(newStack);
    }

    m_size = i + 1;
    return m_nodes[i]->val();
}

} // namespace entwine


/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <vector>

#include <entwine/tree/point-info.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/sized-point-table.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class SimplePointTable : public SizedPointTable
{
public:
    SimplePointTable(DataPool& dataPool, const Schema& schema)
        : SizedPointTable(schema.pdalLayout())
        , m_dataPool(dataPool)
        , m_stack(dataPool)
        , m_nodes()
        , m_size(0)
        , m_blockSize(4096)
    { }

    virtual pdal::PointId addPoint()
    {
        if (m_size == m_stack.size())
        {
            PooledDataStack newStack(m_dataPool.acquire(m_blockSize));
            RawDataNode* node(newStack.head());

            assert(newStack.size() == m_blockSize);

            const std::size_t startSize(m_nodes.size());
            m_nodes.resize(startSize + m_blockSize);

            for (std::size_t i(startSize); i < startSize + m_blockSize; ++i)
            {
                m_nodes[i] = node;
                node = node->next();
            }

            newStack.push(std::move(m_stack));
            m_stack = std::move(newStack);
        }

        return m_size++;
    }

    virtual char* getPoint(pdal::PointId index)
    {
        assert(index < m_nodes.size());
        return m_nodes[index]->val();
    }

    virtual void setField(
            const pdal::Dimension::Detail* dimDetail,
            pdal::PointId index,
            const void* value)
    {
        std::memcpy(getDimension(dimDetail, index), value, dimDetail->size());
    }

    virtual void getField(
            const pdal::Dimension::Detail* dimDetail,
            pdal::PointId index,
            void* value)
    {
        std::memcpy(value, getDimension(dimDetail, index), dimDetail->size());
    }

    // SimplePointTable::stack is destructive, so this doesn't need to be called
    // if the data is consumed via that call.  Otherwise, use clear() to reuse
    // the already-acquired Stack.
    void clear()
    {
        m_size = 0;
    }

    std::size_t size() const
    {
        return m_size;
    }

    PooledDataStack stack()
    {
        m_nodes.erase(m_nodes.begin(), m_nodes.begin() + size());
        PooledDataStack stack(m_stack.pop(m_size));
        m_size = 0;
        return stack;
    }

private:
    char* getDimension(
            const pdal::Dimension::Detail* dimDetail,
            pdal::PointId index)
    {
        return getPoint(index) + dimDetail->offset();
    }

    DataPool& m_dataPool;
    PooledDataStack m_stack;
    std::deque<RawDataNode*> m_nodes;   // m_nodes[0] -> m_stack.head()
    std::size_t m_size;

    const std::size_t m_blockSize;
};

} // namespace entwine


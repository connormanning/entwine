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

#include <pdal/PointTable.hpp>

#include <entwine/tree/point-info.hpp>
#include <entwine/types/schema.hpp>

namespace entwine
{

class PooledPointTable : public pdal::StreamPointTable
{
public:
    PooledPointTable(
            DataPool& dataPool,
            const Schema& schema,
            std::function<void(PooledDataStack)> process);

    virtual pdal::point_count_t capacity() const override;
    virtual void reset() override;

protected:
    virtual char* getPoint(pdal::PointId i) override;

private:
    DataPool& m_dataPool;
    PooledDataStack m_stack;
    std::deque<RawDataNode*> m_nodes;   // m_nodes[0] -> m_stack.head()

    std::function<void(PooledDataStack)> m_process;
    std::size_t m_size;
};

} // namespace entwine


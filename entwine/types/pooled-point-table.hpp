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
#include <entwine/types/structure.hpp>

namespace entwine
{

class PooledPointTable : public pdal::StreamPointTable
{
public:
    // The processing function may acquire nodes from the incoming stack, and
    // can return any that do not need to be kept for reuse.
    PooledPointTable(
            Pools& pools,
            const Schema& schema,
            std::function<PooledInfoStack(PooledInfoStack)> process);

    virtual pdal::point_count_t capacity() const override;
    virtual void reset() override;

protected:
    virtual char* getPoint(pdal::PointId i) override;

private:
    void allocate();

    Pools& m_pools;
    PooledInfoStack m_stack;
    std::deque<RawInfoNode*> m_nodes;   // m_nodes[0] -> m_stack.head()
    std::size_t m_size;

    std::function<PooledInfoStack(PooledInfoStack)> m_process;
};

} // namespace entwine


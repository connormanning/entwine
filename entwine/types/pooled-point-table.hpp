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

class LinkingPointTable : public pdal::StreamPointTable
{
public:
    LinkingPointTable(
            const Schema& schema,
            std::size_t numPoints,
            const char* data)
        : pdal::StreamPointTable(schema.pdalLayout())
        , m_schema(schema)
        , m_numPoints(numPoints)
        , m_data(data)
    { }

    void linkTo(const char* data) { m_data = data; }

    virtual char* getPoint(pdal::PointId index) override
    {
        // :(
        return const_cast<char*>(m_data) + index * m_schema.pointSize();
    }

    virtual pdal::point_count_t capacity() const override
    {
        return m_numPoints;
    }

private:
    const Schema& m_schema;
    const std::size_t m_numPoints;
    const char* m_data;
};

class BinaryPointTable : public pdal::StreamPointTable
{
public:
    BinaryPointTable(Pools& pools, const std::size_t capacity)
        : pdal::StreamPointTable(pools.schema().pdalLayout())
        , m_stack(pools.infoPool().acquire(capacity))
        , m_nodes(capacity)
    {
        PooledDataStack dataStack(pools.dataPool().acquire(capacity));
        RawInfoNode* info(m_stack.head());

        for (std::size_t i(0); i < capacity; ++i)
        {
            m_nodes[i] = info;
            info->construct(dataStack.popOne());
            info = info->next();
        }
    }

    PooledInfoStack acquire();

    virtual pdal::point_count_t capacity() const override
    {
        return m_stack.size();
    }

    virtual char* getPoint(pdal::PointId i) override
    {
        return m_nodes[i]->val().data();
    }

protected:
    PooledInfoStack m_stack;
    std::vector<RawInfoNode*> m_nodes;
};

class PooledPointTable : public pdal::StreamPointTable
{
public:
    // The processing function may acquire nodes from the incoming stack, and
    // can return any that do not need to be kept for reuse.
    PooledPointTable(
            Pools& pools,
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


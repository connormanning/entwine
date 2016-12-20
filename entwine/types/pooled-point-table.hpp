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

#include <pdal/Dimension.hpp>
#include <pdal/PointTable.hpp>

#include <entwine/types/manifest.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class PooledPointTable : public pdal::StreamPointTable
{
public:
    // The processing function may acquire nodes from the incoming stack, and
    // can return any that do not need to be kept for reuse.
    using Process = std::function<Cell::PooledStack(Cell::PooledStack)>;

    PooledPointTable(
            PointPool& pointPool,
            const Schema& schema,
            Process process,
            Origin origin)
        : pdal::StreamPointTable(schema.pdalLayout())
        , m_pointPool(pointPool)
        , m_inSchema(schema)
        , m_process(process)
        , m_dataNodes(pointPool.dataPool())
        , m_cellNodes(pointPool.cellPool())
        , m_refs()
        , m_origin(origin)
        , m_index(0)
        , m_outstanding(0)
    {
        m_refs.reserve(capacity());
        allocate();
    }

    PooledPointTable(
            PointPool& pointPool,
            Process process,
            Origin origin)
        : PooledPointTable(pointPool, pointPool.schema(), process, origin)
    { }

    virtual ~PooledPointTable() { }

    static std::unique_ptr<PooledPointTable> create(
            PointPool& pointPool,
            Process process,
            const Delta* delta,
            Origin origin = invalidOrigin);

    virtual pdal::point_count_t capacity() const override { return 4096; }
    virtual void reset() override;

protected:
    virtual void allocated() { }
    virtual void preprocess() = 0;
    virtual const Schema& outSchema() const { return m_inSchema; }

    std::size_t index() const { return m_index; }
    std::size_t outstanding() const { return m_outstanding; }

    PointPool& m_pointPool;
    const Schema& m_inSchema;
    Process m_process;

    Data::PooledStack m_dataNodes;
    Cell::PooledStack m_cellNodes;

    std::vector<char*> m_refs;

private:
    virtual char* getPoint(pdal::PointId i) override
    {
        m_outstanding = i + 1;
        return m_refs[i];
    }

    void allocate();

    const Origin m_origin;
    std::size_t m_index;
    std::size_t m_outstanding;
};

class NormalPooledPointTable : public PooledPointTable
{
public:
    NormalPooledPointTable(
            PointPool& pointPool,
            PooledPointTable::Process process,
            Origin origin)
        : PooledPointTable(pointPool, process, origin)
    {
        allocated();
    }

private:
    virtual void allocated() override;
    virtual void preprocess() override { }
};

class ConvertingPooledPointTable : public PooledPointTable
{
public:
    ConvertingPooledPointTable(
            PointPool& pointPool,
            std::unique_ptr<Schema> normalizedSchema,
            PooledPointTable::Process process,
            const Delta& delta,
            Origin origin)
        : PooledPointTable(pointPool, *normalizedSchema, process, origin)
        , m_delta(delta)
        , m_preSchema(std::move(normalizedSchema))
        , m_preData(m_preSchema->pointSize() * capacity(), 0)
    {
        for (std::size_t i(0); i < capacity(); ++i)
        {
            m_refs.push_back(m_preData.data() + i * m_preSchema->pointSize());
        }
    }

private:
    virtual void preprocess() override;

    virtual const Schema& outSchema() const override
    {
        return m_pointPool.schema();
    }

    const Delta& m_delta;
    std::unique_ptr<Schema> m_preSchema;
    std::vector<char> m_preData;
};

} // namespace entwine


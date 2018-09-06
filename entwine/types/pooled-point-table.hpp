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

/*
#include <array>
#include <cassert>

#include <pdal/Dimension.hpp>
#include <pdal/PointTable.hpp>
#include <pdal/Reader.hpp>
#include <pdal/Streamable.hpp>

#include <entwine/types/binary-point-table.hpp>
#include <entwine/types/files.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/schema.hpp>

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
            Process process,
            Origin origin = invalidOrigin)
        : pdal::StreamPointTable(pointPool.schema().pdalLayout())
        , m_pointPool(pointPool)
        , m_schema(pointPool.schema())
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

    virtual ~PooledPointTable() { }

    virtual pdal::point_count_t capacity() const override { return 8192; }
    virtual void reset() override;

protected:
    virtual char* getPoint(pdal::PointId i) override
    {
        m_outstanding = i + 1;
        return m_refs[i];
    }

    std::size_t index() const { return m_index; }
    std::size_t outstanding() const { return m_outstanding; }

    void allocate();

    PointPool& m_pointPool;
    const Schema& m_schema;
    Process m_process;

    Data::PooledStack m_dataNodes;
    Cell::PooledStack m_cellNodes;

    std::vector<char*> m_refs;

    const Origin m_origin;
    std::size_t m_index;
    std::size_t m_outstanding;
};

class CellTable : public pdal::StreamPointTable
{
public:
    CellTable(PointPool& pool)
        : pdal::StreamPointTable(pool.schema().pdalLayout())
        , m_pool(pool)
        , m_schema(pool.schema())
        , m_cellStack(pool.cellPool())
    { }

    CellTable(PointPool& pool, Cell::PooledStack cellStack)
        : CellTable(pool)
    {
        m_cellStack = std::move(cellStack);
        for (Cell& cell : m_cellStack)
        {
            for (char* data : cell)
            {
                ++m_size;
                m_refs.emplace_back(cell, data);
            }
        }

        using DimId = pdal::Dimension::Id;
        BinaryPointTable ta(m_schema), tb(m_schema);
        const uint64_t ps(m_schema.pointSize());

        std::sort(
                m_refs.begin(),
                m_refs.end(),
                [&ta, &tb, ps](const Ref& a, const Ref& b)
                {
                    ta.setPoint(a.data());
                    tb.setPoint(b.data());

                    double ga(ta.ref().getFieldAs<double>(DimId::GpsTime));
                    double gb(tb.ref().getFieldAs<double>(DimId::GpsTime));

                    return
                        (ga < gb) ||
                        (ga == gb && std::memcmp(a.data(), b.data(), ps) < 0);
                });
    }

    ~CellTable() { m_pool.release(acquire()); }

    Cell::PooledStack acquire()
    {
        m_size = 0;
        return std::move(m_cellStack);
    }

    void resize(std::size_t s)
    {
        m_cellStack = m_pool.cellPool().acquire(s);
        auto dataStack(m_pool.dataPool().acquire(s));

        m_size = s;
        for (auto& cell : m_cellStack)
        {
            cell.push(dataStack.popOne());
            m_refs.emplace_back(cell, cell.uniqueData());
        }
    }

    std::size_t size() const { return m_size; }
    pdal::point_count_t capacity() const override { return size(); }

private:
    virtual pdal::PointId addPoint() override
    {
        throw std::runtime_error("CellTable::addPoint not allowed");
    }

    virtual char* getPoint(pdal::PointId i) override
    {
        return m_refs[i].data();
    }

    virtual void setFieldInternal(
            pdal::Dimension::Id id,
            pdal::PointId index,
            const void* value) override
    {
        const auto dim(pdal::Utils::toNative(id) - 1);
        const char* src(reinterpret_cast<const char*>(value));
        char* pos(m_refs.at(index).data());

        const pdal::Dimension::Detail* d(m_layoutRef.dimDetail(id));
        char* dst(pos + d->offset());
        std::copy(src, src + d->size(), dst);

        if (dim < 3)
        {
            double& v = m_refs[index].cell().point()[dim];
            char* c(reinterpret_cast<char*>(&v));
            std::copy(src, src + sizeof(double), c);
        }
    }

    class Ref
    {
    public:
        Ref(Cell& cell, char* data) : m_cell(&cell), m_data(data) { }

        Cell& cell() { return *m_cell; }
        char* data() { return m_data; }
        const Cell& cell() const { return *m_cell; }
        const char* data() const { return m_data; }

    private:
        Cell* m_cell;
        char* m_data;
    };

    PointPool& m_pool;
    const Schema& m_schema;
    Cell::PooledStack m_cellStack;
    std::vector<Ref> m_refs;
    std::size_t m_size = 0;
};

class StreamReader : public pdal::Reader, public pdal::Streamable
{
public:
    StreamReader(pdal::StreamPointTable& table) : m_table(table) { }

    virtual bool processOne(pdal::PointRef& point) override
    {
        return ++m_index <= m_table.capacity();
    }

    std::string getName() const override { return "readers.stream"; }

private:
    pdal::StreamPointTable& m_table;
    std::size_t m_index = 0;
};

} // namespace entwine
*/


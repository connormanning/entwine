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
        : PooledPointTable(pointPool, process, origin, pointPool.schema())
    { }

    PooledPointTable(
            PointPool& pointPool,
            Process process,
            Origin origin,
            const Schema& outwardSchema)
        : pdal::StreamPointTable(outwardSchema.pdalLayout())
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

    static std::unique_ptr<PooledPointTable> create(
            PointPool& pointPool,
            Process process,
            const Delta* delta,
            Origin origin = invalidOrigin);

    virtual pdal::point_count_t capacity() const override { return 4096; }
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

class ConvertingPointTable : public PooledPointTable
{
public:
    ConvertingPointTable(
            PointPool& pointPool,
            Process process,
            Origin origin,
            const Delta& delta,
            std::unique_ptr<Schema> normalizedSchema)
        : PooledPointTable(pointPool, process, origin, *normalizedSchema)
        , m_points(capacity())
        , m_delta(delta)
        , m_normalizedSchema(std::move(normalizedSchema))
        , m_sizes{ {
            m_schema.find("X").size(),
            m_schema.find("Y").size(),
            m_schema.find("Z").size()
        } }
        , m_offsets{ { 0, m_sizes[0], m_sizes[0] + m_sizes[1] } }
        , m_xyzSize(m_sizes[0] + m_sizes[1] + m_sizes[2])
        , m_xyzNormal(3 * sizeof(double) - m_xyzSize)
    {
        assert(m_schema.find("X").typeString() == "int32");
        assert(m_schema.find("Y").typeString() == "int32");
        assert(m_schema.find("Z").typeString() == "int32");
    }

protected:
    virtual void setFieldInternal(
            pdal::Dimension::Id id,
            pdal::PointId index,
            const void* value) override
    {
        const auto dim(pdal::Utils::toNative(id) - 1);
        const char* src(reinterpret_cast<const char*>(value));

        if (dim >= 3)
        {
            char* pos(getPoint(index));
            const pdal::Dimension::Detail* d(m_layoutRef.dimDetail(id));
            char* dst(pos + d->offset() - m_xyzNormal);
            std::copy(src, src + d->size(), dst);
        }
        else
        {
            char* dst(reinterpret_cast<char*>(&m_points[index][dim]));
            std::copy(src, src + sizeof(double), dst);

            // This would normally occur in getPoint, but if the schema is only
            // XYZ then the above branch will never execute, so we'd think
            // there are no points.  For the XYZ-only special case, we need to
            // include this here.
            m_outstanding = index + 1;
        }
    }

    virtual void getFieldInternal(
            pdal::Dimension::Id id,
            pdal::PointId index,
            void* value) const override
    {
        const auto dim(pdal::Utils::toNative(id) - 1);
        char* dst(reinterpret_cast<char*>(value));

        if (dim >= 3)
        {
            const char* pos(m_refs[index]);
            const pdal::Dimension::Detail* d(m_layoutRef.dimDetail(id));
            const char* src(pos + d->offset() - m_xyzNormal);
            std::copy(src, src + d->size(), dst);
        }
        else
        {
            const double d(m_points[index][dim]);
            const char* src(reinterpret_cast<const char*>(&d));
            std::copy(src, src + sizeof(double), dst);
        }
    }

    virtual void reset() override
    {
        int64_t v(0);

        for (std::size_t i(0); i < outstanding(); ++i)
        {
            const Point& p(m_points[i]);
            char* pos(m_refs[i]);

            for (std::size_t dim(0); dim < 3; ++dim)
            {
                v = std::llround(
                        Point::scale(
                            p[dim],
                            m_delta.scale()[dim],
                            m_delta.offset()[dim]));

                char* dst(pos + m_offsets[dim]);

                if (m_sizes[dim] == 4) insert<int32_t>(v, dst);
                else if (m_sizes[dim] == 8) insert<int64_t>(v, dst);
                else throw std::runtime_error("Invalid XYZ size");
            }
        }

        PooledPointTable::reset();
    }

private:
    template<typename T>
    void insert(T t, char* dst) const
    {
        const char* src(reinterpret_cast<const char*>(&t));
        std::copy(src, src + sizeof(T), dst);
    }

    std::vector<Point> m_points;
    const Delta& m_delta;
    std::unique_ptr<Schema> m_normalizedSchema;
    std::array<std::size_t, 3> m_sizes;
    std::array<std::size_t, 3> m_offsets;
    std::size_t m_xyzSize;
    std::size_t m_xyzNormal;
};

class CellTable : public pdal::StreamPointTable
{
    static constexpr std::size_t m_blockSize = 4096;

public:
    CellTable(PointPool& pool, std::unique_ptr<Schema> outwardSchema)
        : pdal::StreamPointTable(outwardSchema->pdalLayout())
        , m_pool(pool)
        , m_schema(pool.schema())
        , m_delta(*pool.delta())
        , m_cellStack(pool.cellPool())
        , m_outwardSchema(std::move(outwardSchema))
        , m_sizes{ {
            m_schema.find("X").size(),
            m_schema.find("Y").size(),
            m_schema.find("Z").size()
        } }
        , m_offsets{ { 0, m_sizes[0], m_sizes[0] + m_sizes[1] } }
        , m_xyzSize(m_sizes[0] + m_sizes[1] + m_sizes[2])
        , m_xyzNormal(3 * sizeof(double) - m_xyzSize)
    { }

    CellTable(
            PointPool& pool,
            Cell::PooledStack cellStack,
            std::unique_ptr<Schema> outwardSchema)
        : CellTable(pool, std::move(outwardSchema))
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

        /*
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
        */
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
        return m_refs.at(i).data();
    }

    virtual void setFieldInternal(
            pdal::Dimension::Id id,
            pdal::PointId index,
            const void* value) override
    {
        const auto dim(pdal::Utils::toNative(id) - 1);
        const char* src(reinterpret_cast<const char*>(value));
        char* pos(m_refs.at(index).data());

        if (dim >= 3)
        {
            const pdal::Dimension::Detail* d(m_layoutRef.dimDetail(id));
            char* dst(pos + d->offset() - m_xyzNormal);
            std::copy(src, src + d->size(), dst);
        }
        else
        {
            // First set the scaled value into the Cell's Point.
            double& v(m_refs[index].cell().point()[dim]);
            char* dbl(reinterpret_cast<char*>(&v));
            std::copy(src, src + sizeof(double), dbl);

            v = std::llround(
                    Point::scale(
                        v,
                        m_delta.scale()[dim],
                        m_delta.offset()[dim]));

            // Then copy the scaled value into the binary point data.
            char* dst(pos + m_offsets[dim]);

            if (m_sizes[dim] == 4) insert<int32_t>(v, dst);
            else if (m_sizes[dim] == 8) insert<int64_t>(v, dst);
            else throw std::runtime_error("Invalid XYZ size");
        }
    }

    template<typename T>
    void insert(T t, char* dst) const
    {
        const char* src(reinterpret_cast<const char*>(&t));
        std::copy(src, src + sizeof(T), dst);
    }

    virtual void getFieldInternal(
            pdal::Dimension::Id id,
            pdal::PointId index,
            void* value) const override
    {
        const auto dim(pdal::Utils::toNative(id) - 1);
        char* dst(reinterpret_cast<char*>(value));

        if (dim >= 3)
        {
            const char* pos(m_refs[index].data());
            const pdal::Dimension::Detail* d(m_layoutRef.dimDetail(id));
            const char* src(pos + d->offset() - m_xyzNormal);
            std::copy(src, src + d->size(), dst);
        }
        else
        {
            const double d(
                    Point::unscale(
                        m_refs[index].cell().point()[dim],
                        m_delta.scale()[dim],
                        m_delta.offset()[dim]));
            const char* src(reinterpret_cast<const char*>(&d));
            std::copy(src, src + sizeof(double), dst);
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
    const Delta& m_delta;
    Cell::PooledStack m_cellStack;
    std::vector<Ref> m_refs;
    std::size_t m_size = 0;

    std::unique_ptr<Schema> m_outwardSchema;
    std::array<std::size_t, 3> m_sizes;
    std::array<std::size_t, 3> m_offsets;
    std::size_t m_xyzSize;
    std::size_t m_xyzNormal;
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


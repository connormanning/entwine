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

    PooledPointTable(PointPool& pointPool, Process process, Origin origin)
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
    std::size_t index() const { return m_index; }
    std::size_t outstanding() const { return m_outstanding; }

    PointPool& m_pointPool;
    const Schema& m_schema;
    Process m_process;

    Data::PooledStack m_dataNodes;
    Cell::PooledStack m_cellNodes;

    std::vector<char*> m_refs;

protected:
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
        assert(m_schema.find("X").typeString() == "signed");
        assert(m_schema.find("Y").typeString() == "signed");
        assert(m_schema.find("Z").typeString() == "signed");
    }

protected:
    virtual void setFieldInternal(
            pdal::Dimension::Id id,
            pdal::PointId index,
            const void* value) override
    {
        const auto dim(pdal::Utils::toNative(id) - 1);

        if (dim >= 3)
        {
            char* pos(getPoint(index));
            const pdal::Dimension::Detail* d(m_layoutRef.dimDetail(id));
            const char* src(reinterpret_cast<const char*>(value));
            char* dst(pos + d->offset() - m_xyzNormal);
            std::copy(src, src + d->size(), dst);
        }
        else
        {
            m_points[index][dim] = *reinterpret_cast<const double*>(value);
        }
    }

    virtual void getFieldInternal(
            pdal::Dimension::Id id,
            pdal::PointId index,
            void* value) const override
    {
        const auto dim(pdal::Utils::toNative(id) - 1);

        if (dim >= 3)
        {
            const char* pos(m_refs[index]);
            const pdal::Dimension::Detail* d(m_layoutRef.dimDetail(id));
            const char* src(pos + d->offset() - m_xyzNormal);
            std::copy(src, src + d->size(), reinterpret_cast<char*>(value));
        }
        else
        {
            assert(d->size() == sizeof(double));
            *reinterpret_cast<double*>(value) = m_points[index][dim];
        }
    }

    template<typename T>
    void setConverted(T value, char* dst)
    {
        *reinterpret_cast<T*>(dst) = value;
    }

    virtual void reset() override
    {
        int64_t v(0);
        for (std::size_t i(0); i < outstanding(); ++i)
        {
            const Point& p(m_points[i]);
            char* dst(m_refs[i]);

            for (std::size_t dim(0); dim < 3; ++dim)
            {
                v = std::llround(
                        Point::scale(
                            p[dim],
                            m_delta.scale()[dim],
                            m_delta.offset()[dim]));

                if (m_sizes[dim] == 4)
                {
                    *reinterpret_cast<int32_t*>(dst + m_offsets[dim]) = v;
                }
                else if (m_sizes[dim] == 8)
                {
                    *reinterpret_cast<int64_t*>(dst + m_offsets[dim]) = v;
                }
                else
                {
                    throw std::runtime_error("Invalid XYZ size");
                }
            }
        }

        PooledPointTable::reset();
    }

private:
    std::vector<Point> m_points;
    const Delta& m_delta;
    std::unique_ptr<Schema> m_normalizedSchema;
    std::array<std::size_t, 3> m_sizes;
    std::array<std::size_t, 3> m_offsets;
    std::size_t m_xyzSize;
    std::size_t m_xyzNormal;
};

} // namespace entwine


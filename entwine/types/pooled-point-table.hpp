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
    // virtual const Schema& outSchema() const { return m_schema; }

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
        char* pos(getPoint(index));
        const auto dim(pdal::Utils::toNative(id) - 1);

        if (dim >= 3)
        {
            const pdal::Dimension::Detail* d(m_layoutRef.dimDetail(id));
            const char* src(reinterpret_cast<const char*>(value));
            char* dst(pos + d->offset() - m_xyzNormal);
            std::copy(src, src + d->size(), dst);
        }
        else
        {
            const int64_t v(
                    std::llround(
                        Point::scale(
                            *reinterpret_cast<const double*>(value),
                            m_delta.scale()[dim],
                            m_delta.offset()[dim])));

            char* dst(pos + m_offsets[dim]);

            if (m_sizes[dim] == 4)      setConverted<int32_t>(v, dst);
            else if (m_sizes[dim] == 8) setConverted<int64_t>(v, dst);
            else throw std::runtime_error("Invalid XYZ size");
        }
    }

    template<typename T>
    void setConverted(T value, char* dst)
    {
        const char* src(reinterpret_cast<const char*>(&value));
        std::copy(src, src + sizeof(src), dst);
    }

private:
    const Delta& m_delta;
    std::unique_ptr<Schema> m_normalizedSchema;
    std::array<std::size_t, 3> m_sizes;
    std::array<std::size_t, 3> m_offsets;
    std::size_t m_xyzSize;
    std::size_t m_xyzNormal;
};

} // namespace entwine


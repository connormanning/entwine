/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <entwine/io/io.hpp>

#include <entwine/types/binary-point-table.hpp>

namespace entwine
{

class Ref
{
public:
    Ref(const Cell& cell, const char* data) : m_cell(&cell), m_data(data) { }

    const Cell& cell() { return *m_cell; }
    // char* data() { return m_data; }
    const Cell& cell() const { return *m_cell; }
    const char* data() const { return m_data; }

private:
    const Cell* m_cell;
    const char* m_data;
};

class Binary : public DataIo
{
public:
    Binary(const Metadata& m) : DataIo(m) { }

    virtual std::string type() const override { return "binary"; }

    virtual void write(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pointPool,
            const std::string& filename,
            Cell::PooledStack&& cells,
            uint64_t np) const override;

    virtual Cell::PooledStack read(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pointPool,
            const std::string& filename) const override;

protected:
    std::vector<char> getBuffer(
            const Cell::PooledStack& cells,
            uint64_t np) const
    {
        std::vector<char> buffer;
        const uint64_t pointSize(m_metadata.schema().pointSize());
        buffer.reserve(np * pointSize);

        BinaryPointTable ta(m_metadata.schema()), tb(m_metadata.schema());
        std::vector<Ref> refs;
        refs.reserve(np);
        for (const Cell& cell : cells)
        {
            for (const char* data : cell)
            {
                refs.emplace_back(cell, data);
            }
        }

        using DimId = pdal::Dimension::Id;
        std::sort(
                refs.begin(),
                refs.end(),
                [&ta, &tb](const Ref& a, const Ref& b)
                {
                    ta.setPoint(a.data());
                    tb.setPoint(b.data());
                    return
                        ta.ref().getFieldAs<double>(DimId::GpsTime) <
                        tb.ref().getFieldAs<double>(DimId::GpsTime);
                });

        for (const Ref& ref : refs)
        {
            buffer.insert(buffer.end(), ref.data(), ref.data() + pointSize);
        }

        return buffer;
    }

    Cell::PooledStack getCells(
            PointPool& pool,
            const std::vector<char>& buffer) const;

    std::vector<char> getBuffer(
            const arbiter::Endpoint& out,
            const std::string& filename) const
    {
        return *ensureGet(out, filename);
    }

    virtual void writeBuffer(
            const arbiter::Endpoint& out,
            const std::string filename,
            const std::vector<char>& buffer) const
    {
        ensurePut(out, filename, buffer);
    }
};

} // namespace entwine


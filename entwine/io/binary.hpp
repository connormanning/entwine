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
            uint64_t np) const;

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

    template<typename T>
    void append(std::vector<char>& buffer, T v) const
    {
        const char* cpos(reinterpret_cast<const char*>(&v));
        const char* cend(cpos + sizeof(T));
        buffer.insert(buffer.end(), cpos, cend);
    }
};

} // namespace entwine


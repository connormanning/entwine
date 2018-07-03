/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/io/binary.hpp>

#include <pdal/PointRef.hpp>

#include <entwine/types/binary-point-table.hpp>
#include <entwine/util/executor.hpp>

namespace entwine
{

void Binary::write(
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        PointPool& pointPool,
        const std::string& filename,
        Cell::PooledStack&& cells,
        const uint64_t np) const
{
    writeBuffer(out, filename + ".bin", getBuffer(cells, np));
    pointPool.release(std::move(cells));
}

Cell::PooledStack Binary::read(
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        PointPool& pool,
        const std::string& filename) const
{
    return getCells(pool, getBuffer(out, filename + ".bin"));
}

std::vector<char> Binary::getBuffer(
        const Cell::PooledStack& cells,
        uint64_t np) const
{
    std::vector<char> buffer;
    const uint64_t ps(m_metadata.schema().pointSize());
    buffer.reserve(np * ps);

    using DimId = pdal::Dimension::Id;

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

    assert(refs.size() == np);

    std::sort(
            refs.begin(),
            refs.end(),
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

    for (const Ref& ref : refs)
    {
        buffer.insert(buffer.end(), ref.data(), ref.data() + ps);
    }

    return buffer;
}

Cell::PooledStack Binary::getCells(
        PointPool& pool,
        const std::vector<char>& buffer) const
{
    const uint64_t pointSize(m_metadata.schema().pointSize());
    const uint64_t np(buffer.size() / pointSize);

    assert(buffer.size() % pointSize == 0);

    Cell::PooledStack cellStack(pool.cellPool().acquire(np));
    Data::PooledStack dataStack(pool.dataPool().acquire(np));

    BinaryPointTable table(m_metadata.schema());
    pdal::PointRef pr(table, 0);

    const char* end(buffer.data() + buffer.size());

    Cell::RawNode* cell(cellStack.head());

    for (const char* pos(buffer.data()); pos < end; pos += pointSize)
    {
        assert(dataStack.size());
        auto data(dataStack.popOne());
        std::copy(pos, pos + pointSize, *data);
        table.setPoint(*data);

        assert(cell);
        cell->val().set(pr, std::move(data));
        cell = cell->next();
    }

    assert(!cell);
    assert(dataStack.empty());

    return cellStack;
}

} // namespace entwine


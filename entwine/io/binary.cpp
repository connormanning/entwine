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
}

Cell::PooledStack Binary::read(
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        PointPool& pool,
        const std::string& filename) const
{
    return getCells(pool, getBuffer(out, filename + ".bin"));
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
        table.setPoint(pos);

        assert(cell);
        cell->val().set(pr, std::move(data));
        cell = cell->next();
    }

    assert(!cell);
    assert(dataStack.empty());

    return cellStack;
}

} // namespace entwine


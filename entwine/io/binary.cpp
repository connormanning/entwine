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

/*
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

    const Schema& outSchema(m_metadata.outSchema());
    const uint64_t ps(outSchema.pointSize());
    buffer.reserve(np * ps);

    using DimId = pdal::Dimension::Id;

    const Schema& schema(m_metadata.schema());
    BinaryPointTable ta(schema);
    BinaryPointTable tb(schema);

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

    const auto s(m_metadata.delta()->scale());
    const auto o(m_metadata.delta()->offset());

    using DimType = pdal::Dimension::Type;

    std::array<DimType, 3> types = { {
        outSchema.find(pdal::Dimension::Id::X).type(),
        outSchema.find(pdal::Dimension::Id::Y).type(),
        outSchema.find(pdal::Dimension::Id::Z).type()
    } };

    const uint64_t inXyzSize(
            schema.find(pdal::Dimension::Id::X).size() +
            schema.find(pdal::Dimension::Id::Y).size() +
            schema.find(pdal::Dimension::Id::Z).size());

    for (const Ref& ref : refs)
    {
        const auto& p(ref.cell().point());

        for (std::size_t i(0); i < 3; ++i)
        {
            const double v(std::llround(Point::scale(p[i], s[i], o[i])));
            switch (types[i])
            {
                case DimType::Signed8:      append<int8_t>(buffer, v);
                case DimType::Signed16:     append<int16_t>(buffer, v);
                case DimType::Signed32:     append<int32_t>(buffer, v);
                case DimType::Signed64:     append<int64_t>(buffer, v);
                case DimType::Unsigned8:    append<uint8_t>(buffer, v);
                case DimType::Unsigned16:   append<uint16_t>(buffer, v);
                case DimType::Unsigned32:   append<uint32_t>(buffer, v);
                case DimType::Unsigned64:   append<uint64_t>(buffer, v);
                case DimType::Float:        append<float>(buffer, v);
                case DimType::Double:       append<double>(buffer, v);
                default: throw std::runtime_error("Invalid XYZ type");
            }
        }

        buffer.insert(
                buffer.end(),
                ref.data() + inXyzSize,
                ref.data() + m_metadata.schema().pointSize());
    }

    return buffer;
}

Cell::PooledStack Binary::getCells(
        PointPool& pool,
        const std::vector<char>& buffer) const
{
    const Schema inSchema(m_metadata.outSchema());
    BinaryPointTable inTable(inSchema);
    pdal::PointRef inPr(inTable, 0);

    const uint64_t pointSize(inSchema.pointSize());
    const uint64_t np(buffer.size() / pointSize);
    assert(buffer.size() % pointSize == 0);

    const Delta* delta(m_metadata.delta());
    const Scale s(delta ? delta->scale() : 1);
    const Offset o(delta ? delta->offset() : 0);

    Cell::PooledStack cellStack(pool.cellPool().acquire(np));
    Data::PooledStack dataStack(pool.dataPool().acquire(np));

    const Schema& schema(m_metadata.schema());
    BinaryPointTable table(schema);
    pdal::PointRef pr(table, 0);

    Cell::RawNode* cell(cellStack.head());

    const uint64_t inXyzSize(
            inSchema.find(pdal::Dimension::Id::X).size() +
            inSchema.find(pdal::Dimension::Id::Y).size() +
            inSchema.find(pdal::Dimension::Id::Z).size());

    const uint64_t outXyzSize(
            schema.find(pdal::Dimension::Id::X).size() +
            schema.find(pdal::Dimension::Id::Y).size() +
            schema.find(pdal::Dimension::Id::Z).size());

    const char* end(buffer.data() + buffer.size());

    for (const char* pos(buffer.data()); pos < end; pos += pointSize)
    {
        assert(dataStack.size());
        auto data(dataStack.popOne());

        inTable.setPoint(pos);

        const Point scaledPoint = Point(
                inPr.getFieldAs<double>(pdal::Dimension::Id::X),
                inPr.getFieldAs<double>(pdal::Dimension::Id::Y),
                inPr.getFieldAs<double>(pdal::Dimension::Id::Z));

        const Point p(Point::unscale(scaledPoint, s, o));

        table.setPoint(*data);

        pr.setField(pdal::Dimension::Id::X, p.x);
        pr.setField(pdal::Dimension::Id::Y, p.y);
        pr.setField(pdal::Dimension::Id::Z, p.z);

        std::copy(pos + inXyzSize, pos + pointSize, *data + outXyzSize);

        assert(cell);
        cell->get().set(pr, std::move(data));
        cell = cell->next();
    }

    assert(!cell);
    assert(dataStack.empty());

    return cellStack;
}

} // namespace entwine
*/


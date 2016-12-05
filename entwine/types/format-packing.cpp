/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/format-packing.hpp>

#include <entwine/types/binary-point-table.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/format.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/util/compression.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    void append(std::vector<char>& data, const std::vector<char>& add)
    {
        data.insert(data.end(), add.begin(), add.end());
    }
}

std::vector<char> Packer::buildTail() const
{
    std::vector<char> tail;

    for (TailField field : m_fields)
    {
        switch (field)
        {
            case TailField::ChunkType: append(tail, chunkType()); break;
            case TailField::NumPoints: append(tail, numPoints()); break;
            case TailField::NumBytes: append(tail, numBytes()); break;
        }
    }

    return tail;
}

Unpacker::Unpacker(
        const Format& format,
        std::unique_ptr<std::vector<char>> data)
    : m_format(format)
    , m_data(std::move(data))
{
    const auto& fields(m_format.tailFields());

    // Since we're unpacking from the back, the fields are in reverse order.
    for (auto it(fields.rbegin()); it != fields.rend(); ++it)
    {
        switch (*it)
        {
            case TailField::ChunkType: extractChunkType(); break;
            case TailField::NumPoints: extractNumPoints(); break;
            case TailField::NumBytes: extractNumBytes(); break;
        }
    }

    if (m_numBytes)
    {
        if (*m_numBytes != m_data->size())
        {
            throw std::runtime_error("Incorrect byte count");
        }
    }

    if (m_format.compress() && !m_numPoints)
    {
        throw std::runtime_error("Cannot decompress without numPoints");
    }

    if (!m_numPoints)
    {
        m_numPoints = makeUnique<std::size_t>(
                m_data->size() / m_format.schema().pointSize());
    }
}

std::unique_ptr<std::vector<char>>&& Unpacker::acquireBytes()
{
    if (m_format.compress())
    {
        m_data = Compression::decompress(
                *m_data,
                m_format.schema(),
                numPoints());
    }

    return std::move(m_data);
}

Cell::PooledStack Unpacker::acquireCells(PointPool& pointPool)
{
    const auto np(numPoints());
    if (m_format.compress())
    {
        auto d(Compression::decompress(*m_data, np, pointPool));
        m_data.reset();
        return d;
    }
    else
    {
        const std::size_t pointSize(m_format.schema().pointSize());
        BinaryPointTable table(m_format.schema());
        pdal::PointRef pointRef(table, 0);

        Data::PooledStack dataStack(pointPool.dataPool().acquire(np));
        Cell::PooledStack cellStack(pointPool.cellPool().acquire(np));

        Cell::RawNode* cell(cellStack.head());

        const char* pos(m_data->data());

        for (std::size_t i(0); i < np; ++i)
        {
            table.setPoint(pos);

            Data::PooledNode data(dataStack.popOne());
            std::copy(pos, pos + pointSize, *data);

            (*cell)->set(pointRef, std::move(data));
            cell = cell->next();

            pos += pointSize;
        }

        return cellStack;
    }
}

} // namespace entwine


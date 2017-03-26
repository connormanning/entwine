/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cassert>

#include <entwine/types/binary-point-table.hpp>
#include <entwine/types/chunk-storage/chunk-storage.hpp>

namespace entwine
{

class BinaryStorage : public ChunkStorage
{
public:
    BinaryStorage(const Metadata& m, const Json::Value& json = Json::nullValue)
        : ChunkStorage(m)
    {
        if (json.isNull())
        {
            m_tailFields = std::vector<TailField>{
                TailField::NumPoints,
                TailField::NumBytes
            };
        }
        else
        {
            for (const auto& f : json["tail"])
            {
                m_tailFields.push_back(toTailField(f.asString()));
            }
        }
    }

    virtual void write(Chunk& chunk) const override
    {
        auto data(buildData(chunk));
        const auto& schema(chunk.schema());
        const std::size_t numPoints(data.size() / schema.pointSize());
        append(data, buildTail(chunk, numPoints));
        ensurePut(chunk, m_metadata.basename(chunk.id()), data);
    }

    virtual Cell::PooledStack read(
            const arbiter::Endpoint& endpoint,
            PointPool& pool,
            const Id& id) const override
    {
        auto data(io::ensureGet(endpoint, m_metadata.basename(id)));
        const Tail tail(*data, m_tailFields);
        const char* pos(data->data());

        const Schema& schema(pool.schema());
        const std::size_t pointSize(schema.pointSize());
        const std::size_t numPoints(data->size() / pointSize);
        const std::size_t numBytes(data->size() + tail.size());
        BinaryPointTable table(schema);
        pdal::PointRef pointRef(table, 0);

        if (pointSize * numPoints != data->size())
        {
            throw std::runtime_error("Invalid binary chunk size");
        }
        if (tail.numPoints() && tail.numPoints() != numPoints)
        {
            throw std::runtime_error("Invalid binary chunk numPoints");
        }
        if (tail.numBytes() && tail.numBytes() != numBytes)
        {
            throw std::runtime_error("Invalid binary chunk numBytes");
        }

        Data::PooledStack dataStack(pool.dataPool().acquire(numPoints));
        Cell::PooledStack cellStack(pool.cellPool().acquire(numPoints));

        for (Cell& cell : cellStack)
        {
            table.setPoint(pos);
            Data::PooledNode dataNode(dataStack.popOne());
            std::copy(pos, pos + pointSize, *dataNode);

            cell.set(pointRef, std::move(dataNode));
            pos += pointSize;
        }

        assert(dataStack.empty());
        return cellStack;
    }

    virtual Json::Value toJson() const override
    {
        Json::Value json;
        for (const TailField f : m_tailFields) json["tail"].append(toString(f));
        return json;
    }

protected:
    std::vector<char> buildData(Chunk& chunk) const
    {
        Cell::PooledStack cellStack(chunk.acquire());
        Data::PooledStack dataStack(chunk.pool().dataPool());
        for (Cell& cell : cellStack) dataStack.push(cell.acquire());
        cellStack.release();

        const std::size_t pointSize(chunk.schema().pointSize());

        std::vector<char> data;
        data.reserve(
                dataStack.size() * pointSize +
                buildTail(chunk, dataStack.size()).size());

        for (const char* d : dataStack)
        {
            data.insert(data.end(), d, d + pointSize);
        }

        return data;
    }

    std::vector<char> buildTail(
            const Chunk& chunk,
            const std::size_t numPoints,
            std::size_t numBytes = 0) const
    {
        using Data = std::vector<char>;
        Data tail;

        std::size_t tailSize(0);
        for (TailField field : m_tailFields)
        {
            switch (field)
            {
                case TailField::ChunkType:
                    ++tailSize;
                    break;
                case TailField::NumPoints:
                case TailField::NumBytes:
                    tailSize += 8;
                    break;
            }
        }

        if (!numBytes) numBytes = numPoints * chunk.schema().pointSize();
        numBytes += tailSize;

        for (TailField field : m_tailFields)
        {
            switch (field)
            {
                case TailField::ChunkType:
                    append(tail, Data{ static_cast<char>(chunk.type()) });
                    break;
                case TailField::NumPoints:
                    append(tail, numPoints);
                    break;
                case TailField::NumBytes:
                    append(tail, numBytes);
                    break;
            }
        }

        return tail;
    }

    void append(std::vector<char>& data, const std::vector<char>& add) const
    {
        data.insert(data.end(), add.begin(), add.end());
    }

    void append(std::vector<char>& data, uint64_t v) const
    {
        const char* pos(reinterpret_cast<const char*>(&v));
        append(data, std::vector<char>{ pos, pos + sizeof(uint64_t) });
    }

    TailFieldList m_tailFields;
};

} // namespace entwine


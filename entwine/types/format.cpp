/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/format.hpp>

#include <numeric>

#include <entwine/types/pooled-point-table.hpp>
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

    std::vector<std::string> fieldsFromJson(const Json::Value& json)
    {
        return std::accumulate(
                json.begin(),
                json.end(),
                std::vector<std::string>(),
                [](const std::vector<std::string>& in, const Json::Value& f)
                {
                    auto out(in);
                    out.push_back(f.asString());
                    return out;
                });
    }
}

Format::Format(
        const Schema& schema,
        bool trustHeaders,
        bool compress,
        std::vector<std::string> tailFields,
        std::string srs)
    : m_schema(schema)
    , m_trustHeaders(trustHeaders)
    , m_compress(compress)
    , m_tailFields(std::accumulate(
                tailFields.begin(),
                tailFields.end(),
                TailFields(),
                [](const TailFields& in, const std::string& v)
                {
                    auto out(in);
                    auto it(
                            std::find_if(
                                tailFieldNames.begin(),
                                tailFieldNames.end(),
                                [&v](const TailFieldLookup::value_type& p)
                                {
                                    return p.second == v;
                                }));

                    if (it == tailFieldNames.end())
                    {
                        throw std::runtime_error("Invalid tail field " + v);
                    }
                    out.push_back(it->first);
                    return out;
                }))
    , m_srs(srs)
{
    for (const auto f : m_tailFields)
    {
        if (std::count(m_tailFields.begin(), m_tailFields.end(), f) > 1)
        {
            throw std::runtime_error("Identical tail fields detected");
        }
    }

    const bool hasNumPoints(
            std::count(
                m_tailFields.begin(),
                m_tailFields.end(),
                TailField::NumPoints));

    if (m_compress && !hasNumPoints)
    {
        throw std::runtime_error(
                "Cannot specify compression without numPoints");
    }
}

Format::Format(const Schema& schema, const Json::Value& json)
    : Format(
            schema,
            json["trustHeaders"].asBool(),
            json["compress"].asBool(),
            fieldsFromJson(json["tail"]),
            json["srs"].asString())
{ }

std::unique_ptr<std::vector<char>> Format::pack(
        Data::PooledStack dataStack,
        const ChunkType chunkType) const
{
    std::unique_ptr<std::vector<char>> data;
    const std::size_t numPoints(dataStack.size());
    const std::size_t pointSize(m_schema.pointSize());

    if (m_compress)
    {
        Compressor compressor(m_schema, dataStack.size());
        for (const char* pos : dataStack) compressor.push(pos, pointSize);
        data = compressor.data();
    }
    else
    {
        data = makeUnique<std::vector<char>>();
        data->reserve(numPoints * pointSize);
        for (const char* pos : dataStack)
        {
            data->insert(data->end(), pos, pos + pointSize);
        }
    }

    assert(data);
    dataStack.reset();

    Packer packer(m_tailFields, *data, numPoints, chunkType);
    append(*data, packer.buildTail());

    return data;
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

    if (m_format.compress() && !numPoints())
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
    if (m_format.compress())
    {
        auto d(Compression::decompress(*m_data, numPoints(), pointPool));
        m_data.reset();
        return d;
    }
    else
    {
        const std::size_t pointSize(m_format.schema().pointSize());
        BinaryPointTable table(m_format.schema());
        pdal::PointRef pointRef(table, 0);

        Data::PooledStack dataStack(pointPool.dataPool().acquire(numPoints()));
        Cell::PooledStack cellStack(pointPool.cellPool().acquire(numPoints()));

        Cell::RawNode* cell(cellStack.head());

        const char* pos(m_data->data());

        for (std::size_t i(0); i < numPoints(); ++i)
        {
            table.setPoint(pos);

            Data::PooledNode data(dataStack.popOne());
            std::copy(pos, pos + pointSize, *data);

            (*cell)->set(pointRef, std::move(data));
            cell = cell->next();
        }

        return cellStack;
    }
}

} // namespace entwine


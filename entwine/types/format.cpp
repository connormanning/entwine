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

#include <entwine/types/binary-point-table.hpp>
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
        const Metadata& metadata,
        const bool trustHeaders,
        const bool compress,
        const HierarchyCompression hierarchyCompression,
        const std::vector<std::string> tailFields,
        const std::string srs)
    : m_metadata(metadata)
    , m_trustHeaders(trustHeaders)
    , m_compress(compress)
    , m_hierarchyCompression(hierarchyCompression)
    , m_tailFields(std::accumulate(
                tailFields.begin(),
                tailFields.end(),
                TailFields(),
                [](const TailFields& in, const std::string& v)
                {
                    auto out(in);
                    out.push_back(tailFieldFromName(v));
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

Format::Format(const Metadata& metadata, const Json::Value& json)
    : Format(
            metadata,
            json["trustHeaders"].asBool(),
            json["compress"].asBool(),
            hierarchyCompressionFromName(json["compress-hierarchy"].asString()),
            fieldsFromJson(json["tail"]),
            json["srs"].asString())
{ }

std::unique_ptr<std::vector<char>> Format::pack(
        Data::PooledStack dataStack,
        const ChunkType chunkType) const
{
    std::unique_ptr<std::vector<char>> data;
    const std::size_t numPoints(dataStack.size());
    const std::size_t pointSize(schema().pointSize());

    if (m_compress)
    {
        Compressor compressor(m_metadata.schema(), dataStack.size());
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

const Metadata& Format::metadata() const { return m_metadata; }
const Schema& Format::schema() const { return m_metadata.schema(); }

} // namespace entwine


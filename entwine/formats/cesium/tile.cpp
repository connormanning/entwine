/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/formats/cesium/tile.hpp>

#include <cassert>
#include <iostream>

namespace entwine
{
namespace cesium
{

std::vector<char> Tile::asBinary() const
{
    std::vector<char> data;

    const Json::Value featureTableJson(m_featureTable.getJson());
    Json::FastWriter writer;
    std::string featureTableString(writer.write(featureTableJson));

    if (featureTableString.size() % 8)
    {
        featureTableString.insert(
                featureTableString.end(),
                8 - (featureTableString.size() % 8),
                ' ');
    }

    static const std::size_t headerSize(28);

    const std::size_t byteLength(
            headerSize +
            featureTableString.size() +
            m_featureTable.bytes());

    data.reserve(byteLength);

    const std::string magic("pnts");
    data.insert(data.end(), magic.begin(), magic.end());

    const int version(1);
    append(data, version);

    append(data, byteLength);
    append(data, featureTableString.size());
    append(data, m_featureTable.bytes());
    append(data, 0);    // batchTableJsonByteLength.
    append(data, 0);    // batchTableBinaryByteLength.

    data.insert(
            data.end(),
            featureTableString.begin(),
            featureTableString.end());

    m_featureTable.appendBinary(data);

    assert(data.size() == byteLength);

    return data;
}

Tile::Tile(const std::string& dataPath)
{
    arbiter::Arbiter a;
    auto data(a.getBinary(dataPath));
    char* pos(data.data());

    std::cout << "Buffer size: " << data.size() << std::endl;

    // Parse the 28-byte header.

    const std::string magic(pos, pos + 4);
    pos += 4;
    if (magic != "pnts")
    {
        throw std::runtime_error("Invalid magic: " + magic);
    }

    const auto version(extract(pos));
    if (version != 1)
    {
        std::cout << "Version: " << version << std::endl;
        throw std::runtime_error("Invalid version");
    }

    const std::size_t byteLength(extract(pos));
    std::cout << "Total size: " << byteLength << std::endl;

    if (data.size() != byteLength)
    {
        std::cout << data.size() << " != " << byteLength << std::endl;
        throw std::runtime_error("Invalid size");
    }

    const std::size_t featureTableJsonSize(extract(pos));
    std::cout << "Feature json size: " << featureTableJsonSize << std::endl;

    const std::size_t featureTableBinarySize(extract(pos));
    std::cout << "Feature binary size: " << featureTableBinarySize <<
        std::endl;

    const std::size_t batchTableJsonSize(extract(pos));
    std::cout << "Batch json size: " << batchTableJsonSize << std::endl;

    const std::size_t batchTableBinarySize(extract(pos));
    std::cout << "Batch binary size: " << batchTableBinarySize << std::endl;

    if (pos - data.data() != 28)
    {
        throw std::runtime_error("Invalid position post-header");
    }

    // Header is parsed, move on to content.

    const std::string featureTableJsonString(
            pos,
            pos + featureTableJsonSize);

    pos += featureTableJsonSize;
    const auto featureTableJson(parse(featureTableJsonString));

    std::cout << "Feature table json: " <<
        featureTableJson.toStyledString() << std::endl;

    m_featureTable = FeatureTable(featureTableJson, pos);
}

} // namespace cesium
} // namespace entwine


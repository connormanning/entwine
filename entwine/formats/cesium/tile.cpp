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

namespace
{

void maybePad(std::string& s)
{
    if (s.size() % 8) s.insert(s.end(), 8 - (s.size() % 8), ' ');
}

}

std::vector<char> Tile::asBinary() const
{
    std::vector<char> data;
    Json::FastWriter writer;

    std::string featureTableJson;
    std::string batchTableJson;

    if (m_featureTable.bytes())
    {
        featureTableJson = writer.write(m_featureTable.getJson());
    }

    if (m_batchTable.bytes())
    {
        batchTableJson = writer.write(m_batchTable.getJson());
    }

    maybePad(featureTableJson);
    maybePad(batchTableJson);

    static const std::size_t headerSize(28);

    const std::size_t byteLength(
            headerSize +
            featureTableJson.size() +
            m_featureTable.bytes() +
            batchTableJson.size() +
            m_batchTable.bytes());

    data.reserve(byteLength);

    const std::string magic("pnts");
    data.insert(data.end(), magic.begin(), magic.end());

    const int version(1);
    append(data, version);

    append(data, byteLength);
    append(data, featureTableJson.size());
    append(data, m_featureTable.bytes());
    append(data, batchTableJson.size());
    append(data, m_batchTable.bytes());

    data.insert(data.end(), featureTableJson.begin(), featureTableJson.end());
    m_featureTable.appendBinary(data);

    data.insert(data.end(), batchTableJson.begin(), batchTableJson.end());
    m_batchTable.appendBinary(data);

    assert(data.size() == byteLength);

    return data;
}

} // namespace cesium
} // namespace entwine


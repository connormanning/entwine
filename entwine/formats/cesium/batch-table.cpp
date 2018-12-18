/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/formats/cesium/batch-table.hpp>

#include <iostream>
#include <stdexcept>

#include <entwine/formats/cesium/settings.hpp>
#include <entwine/formats/cesium/tile.hpp>

namespace entwine
{
namespace cesium
{

BatchTable::BatchTable(const Metadata& metadata, const TileData& tileData)
    : m_metadata(metadata)
    , m_tileData(tileData)
    , m_table(metadata.schema())
    , m_pointRef(m_table, 0)
{
    const auto& settings = *m_metadata.cesiumSettings();
    const auto& dimNames(settings.batchTableDimensions());
    m_batchReferences.reserve(dimNames.size());
    const auto schema(metadata.schema());
    size_t byteOffset = 0;

    for (const auto& dimName : dimNames)
    {
        DimInfo dimInfo(schema.find(dimName));

        // All properties will be scalar for now.
        m_batchReferences.emplace_back(
                dimName,
                byteOffset,
                BatchReference::findComponentType(dimInfo.type()));
        const auto& ref = m_batchReferences.back();

        // It is necessary to retrieve the data here for later use; the point
        // table's setPoint method cannot be used during calls to appendBinary.
        // To this end, allocate space for the data and copy it over.
        m_data.resize(
                byteOffset + m_tileData.pointIndices.size() * ref.bytes());

        for (const auto index : m_tileData.pointIndices)
        {
            m_table.setPoint(index);
            m_pointRef.getField(
                    m_data.data() + byteOffset,
                    dimInfo.id(),
                    dimInfo.type());
            byteOffset += ref.bytes();
        }
    }
}

Json::Value BatchTable::getJson() const
{
    Json::Value json;

    for (const auto& ref : m_batchReferences)
    {
        json[ref.name()] = ref.getJson();
    }

    return json;
}

void BatchTable::appendBinary(std::vector<char>& data) const
{
    data.insert(data.end(), m_data.begin(), m_data.end());

    // Pad remaining space so that batch table will end at 8-byte boundary
    if (m_data.size() % 8)
    {
        data.insert(data.end(), 8 - (m_data.size() % 8), 'X');
    }
}

std::size_t BatchTable::bytes() const
{
    std::size_t size = m_data.size();

    // Reserve extra bytes for padding to 8-byte boundary
    if (size % 8)
    {
        size += 8 - (size % 8);
    }

    return size;
}

const std::vector<Point>& BatchTable::points() const
{
    return m_tileData.points;
}

} // namespace cesium
} // namespace entwine


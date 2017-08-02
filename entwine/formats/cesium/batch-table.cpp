/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/formats/cesium/feature-table.hpp>

#include <iostream>
#include <stdexcept>

#include <entwine/formats/cesium/tile.hpp>

namespace entwine
{
namespace cesium
{

BatchTable::BatchTable(const TileData& tileData)
    : m_tileData(tileData)
{
}

Json::Value BatchTable::getJson() const
{
    Json::Value json;
    return json;
}

void BatchTable::appendBinary(std::vector<char>& data) const
{
}

std::size_t BatchTable::bytes() const
{
    return 0;
}

} // namespace cesium
} // namespace entwine


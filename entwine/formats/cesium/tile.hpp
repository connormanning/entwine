/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <string>
#include <vector>

#include <json/json.h>

#include <entwine/formats/cesium/batch-table.hpp>
#include <entwine/formats/cesium/feature-table.hpp>
#include <entwine/formats/cesium/tile-info.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>

namespace entwine
{

namespace cesium
{

class TileData
{
public:
    TileData(std::size_t numPoints, bool hasColor, bool hasNormals)
    {
        points.reserve(numPoints);
        if (hasColor) colors.reserve(numPoints);
        if (hasNormals) normals.reserve(numPoints);
    }

    std::vector<Point> points;
    std::vector<Color> colors;
    std::vector<Point> normals;
};

class Tile
{
public:
    Tile(const TileData& tileData)
        : m_featureTable(tileData)
        , m_batchTable(tileData)
    { }

    std::vector<char> asBinary() const;

    const FeatureTable& featureTable() const { return m_featureTable; }
    const BatchTable& batchTable() const { return m_batchTable; }

private:
    void append(std::vector<char>& data, uint32_t v) const
    {
        data.insert(
                data.end(),
                reinterpret_cast<const char*>(&v),
                reinterpret_cast<const char*>(&v + 1));
    }

    FeatureTable m_featureTable;
    BatchTable m_batchTable;
};

} // namespace cesium
} // namespace entwine


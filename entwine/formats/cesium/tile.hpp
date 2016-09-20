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

#include <entwine/formats/cesium/feature-table.hpp>
#include <entwine/formats/cesium/tile-info.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>

namespace entwine
{

namespace cesium
{

class Tile
{
public:
    // Unused but maybe helpful for verification.
    Tile(const std::string& dataPath);

    Tile(
            const std::vector<Point>& points,
            const std::vector<Color>& colors)
        : m_featureTable(points, colors)
    { }

    std::vector<char> asBinary() const;

    const FeatureTable& featureTable() const { return m_featureTable; }

private:
    uint32_t extract(char*& pos)
    {
        uint32_t result(*reinterpret_cast<const uint32_t*>(pos));
        pos += 4;
        return result;
    }

    void append(std::vector<char>& data, uint32_t v) const
    {
        data.insert(
                data.end(),
                reinterpret_cast<const char*>(&v),
                reinterpret_cast<const char*>(&v + 1));
    }

    FeatureTable m_featureTable;
};

class TileData
{
public:
    TileData(std::size_t numPoints, bool hasColor)
    {
        points.reserve(numPoints);
        if (hasColor) colors.reserve(numPoints);
    }

    std::vector<Point> points;
    std::vector<Color> colors;
};

} // namespace cesium
} // namespace entwine


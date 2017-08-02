/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
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

FeatureTable::FeatureTable(const TileData& tileData)
    : m_tileData(tileData)
{
    if (colors().size() && colors().size() != points().size())
    {
        std::cout << colors().size() << " != " << points().size() << std::endl;
        throw std::runtime_error("Invalid colors size");
    }

    if (normals().size() && normals().size() != points().size())
    {
        std::cout << normals().size() << " != " << points().size() << std::endl;
        throw std::runtime_error("Invalid normals size");
    }
}

Json::Value FeatureTable::getJson() const
{
    long byteOffset = 0;

    Json::Value json;
    json["POINTS_LENGTH"] = Json::UInt64(points().size());
    json["POSITION"]["byteOffset"] = Json::UInt64(byteOffset);
    byteOffset += points().size() * 3 * sizeof(float);

    if (colors().size())
    {
        json["RGB"]["byteOffset"] = Json::UInt64(byteOffset);
        byteOffset += colors().size() * 3 * sizeof(uint8_t);
    }

    if (normals().size())
    {
        json["NORMAL"]["byteOffset"] = Json::UInt64(byteOffset);
        byteOffset += normals().size() * 3 * sizeof(float);
    }

    return json;
}

void FeatureTable::appendBinary(std::vector<char>& data) const
{
    float f;
    const char* pos(reinterpret_cast<const char*>(&f));
    const char* end(reinterpret_cast<const char*>(&f + 1));

    for (const Point& p : points())
    {
        f = p.x;
        data.insert(data.end(), pos, end);

        f = p.y;
        data.insert(data.end(), pos, end);

        f = p.z;
        data.insert(data.end(), pos, end);
    }

    for (const Color& c : colors())
    {
        data.push_back(c.r);
        data.push_back(c.g);
        data.push_back(c.b);
    }

    for (const Point& p : normals())
    {
        f = p.x;
        data.insert(data.end(), pos, end);

        f = p.y;
        data.insert(data.end(), pos, end);

        f = p.z;
        data.insert(data.end(), pos, end);
    }
}

std::size_t FeatureTable::bytes() const
{
    return
        m_tileData.points.size() * 3 * sizeof(float) +
        m_tileData.colors.size() * 3 * sizeof(uint8_t) +
        m_tileData.normals.size() * 3 * sizeof(float);
}

const std::vector<Point>& FeatureTable::points() const
{
    return m_tileData.points;
}

const std::vector<Color>& FeatureTable::colors() const
{
    return m_tileData.colors;
}

const std::vector<Point>& FeatureTable::normals() const
{
    return m_tileData.normals;
}

} // namespace cesium
} // namespace entwine


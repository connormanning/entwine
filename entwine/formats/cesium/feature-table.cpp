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

namespace entwine
{
namespace cesium
{

namespace
{
    const double dmin(std::numeric_limits<double>::min());
    const double dmax(std::numeric_limits<double>::max());
}

FeatureTable::FeatureTable(
        const std::vector<Point>& points,
        const std::vector<Color>& colors,
        const std::vector<Point>& normals)
    : m_points(points)
    , m_normals(normals)
    , m_colors(colors)
{
    if (colors.size() && colors.size() != points.size())
    {
        std::cout << colors.size() << " != " << points.size() << std::endl;
        throw std::runtime_error("Invalid colors size");
    }
}

Json::Value FeatureTable::getJson() const
{
    long byteOffset = 0;

    Json::Value json;
    json["POINTS_LENGTH"] = Json::UInt64(m_points.size());
    json["POSITION"]["byteOffset"] = Json::UInt64(byteOffset);
    byteOffset += m_points.size() * 3 * sizeof(float);

    if (m_colors.size())
    {
        json["RGB"]["byteOffset"] = Json::UInt64(byteOffset);
        byteOffset += m_colors.size() * 3 * sizeof(uint8_t);
    }

    if (m_normals.size())
    {
        json["NORMAL"]["byteOffset"] = Json::UInt64(byteOffset);
        byteOffset += m_normals.size() * 3 * sizeof(float);
    }

    return json;
}

void FeatureTable::appendBinary(std::vector<char>& data) const
{
    float f;
    const char* pos(reinterpret_cast<const char*>(&f));
    const char* end(reinterpret_cast<const char*>(&f + 1));

    for (const Point& p : m_points)
    {
        f = p.x;
        data.insert(data.end(), pos, end);

        f = p.y;
        data.insert(data.end(), pos, end);

        f = p.z;
        data.insert(data.end(), pos, end);
    }

    for (const Color& c : m_colors)
    {
        data.push_back(c.r);
        data.push_back(c.g);
        data.push_back(c.b);
    }

    for (const Point& p : m_normals)
    {
        f = p.x;
        data.insert(data.end(), pos, end);

        f = p.y;
        data.insert(data.end(), pos, end);

        f = p.z;
        data.insert(data.end(), pos, end);
    }
}

FeatureTable::FeatureTable(const Json::Value& json, const char* pos)
{
    if (!json.isMember("POINTS_LENGTH"))
    {
        throw std::runtime_error("Required POINTS_LENGTH not found");
    }

    const std::size_t numPoints(json["POINTS_LENGTH"].asUInt64());

    std::cout << "Number of points: " << numPoints << std::endl;

    if (json.isMember("POSITION"))
    {
        std::cout << "Found floating point positions" << std::endl;
        m_points.reserve(numPoints * 3);

        Point center;

        if (json.isMember("RTC_CENTER"))
        {
            const auto& centerJson(json["RTC_CENTER"]);
            center = Point(
                    centerJson[0].asDouble(),
                    centerJson[1].asDouble(),
                    centerJson[2].asDouble());

            std::cout << "Got RTC center: " << center << std::endl;
        }

        const std::size_t pointDataOffset(
                json["POSITION"]["byteOffset"].asUInt64());

        const char* pointPos(pos + pointDataOffset);
        const char* pointEnd(pointPos + numPoints * 3 * sizeof(float));

        for ( ; pointPos < pointEnd; pointPos += 3 * sizeof(float))
        {
            m_points.emplace_back(
                    *reinterpret_cast<const float*>(pointPos + 0),
                    *reinterpret_cast<const float*>(pointPos + 4),
                    *reinterpret_cast<const float*>(pointPos + 8));
        }

        summarizePoints();
    }
    else if (json.isMember("POSITION_QUANTIZED"))
    {
        std::cout << "Found quantized point positions" << std::endl;
    }
    else
    {
        throw std::runtime_error("No POSITION or POSITION_QUANTIZED found");
    }

    if (json.isMember("RGB"))
    {
        m_colors.reserve(numPoints * 3);

        const std::size_t colorDataOffset(
                json["RGB"]["byteOffset"].asUInt64());

        const char* colorPos(pos + colorDataOffset);
        const char* colorEnd(colorPos + numPoints * 3 * sizeof(uint8_t));

        for ( ; colorPos < colorEnd; colorPos += 3 * sizeof(uint8_t))
        {
            m_colors.emplace_back(
                    *(colorPos + 0),
                    *(colorPos + 1),
                    *(colorPos + 2));
        }

        summarizeColors();
    }

    if (json.isMember("NORMAL"))
    {
        std::cout << "Found floating point normals" << std::endl;
        m_normals.reserve(numPoints * 3);

        const std::size_t normalDataOffset(
                json["NORMAL"]["byteOffset"].asUInt64());

        const char* normalPos(pos + normalDataOffset);
        const char* normalEnd(normalPos + numPoints * 3 * sizeof(float));

        for ( ; normalPos < normalEnd; normalPos += 3 * sizeof(float))
        {
            m_normals.emplace_back(
                    *reinterpret_cast<const float*>(normalPos + 0),
                    *reinterpret_cast<const float*>(normalPos + 4),
                    *reinterpret_cast<const float*>(normalPos + 8));
        }

        summarizeNormals();
    }
}

void FeatureTable::summarizePoints() const
{
    Point min(dmax, dmax, dmax);
    Point max(dmin, dmin, dmin);

    for (const Point& p : m_points)
    {
        min = Point::min(min, p);
        max = Point::max(max, p);
    }

    std::cout << "Point min: " << min << std::endl;
    std::cout << "Point max: " << max << std::endl;
}

void FeatureTable::summarizeColors() const
{
    Color min(255, 255, 255);
    Color max(0, 0, 0);

    for (const Color& c : m_colors)
    {
        min = Color::min(min, c);
        max = Color::max(max, c);
    }

    std::cout << "Color min: " << min << std::endl;
    std::cout << "Color max: " << max << std::endl;
}

void FeatureTable::summarizeNormals() const
{
    Point min(dmax, dmax, dmax);
    Point max(dmin, dmin, dmin);

    for (const Point& p : m_normals)
    {
        min = Point::min(min, p);
        max = Point::max(max, p);
    }

    std::cout << "Normal min: " << min << std::endl;
    std::cout << "Normal max: " << max << std::endl;
}

} // namespace cesium
} // namespace entwine


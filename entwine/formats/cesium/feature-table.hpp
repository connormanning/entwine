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

#include <vector>

#include <json/json.h>

#include <entwine/types/point.hpp>

namespace entwine
{
namespace cesium
{

class FeatureTable
{
public:
    FeatureTable() = default;

    FeatureTable(
            const std::vector<Point>& points,
            const std::vector<Color>& colors,
            const std::vector<Point>& normals);

    // Not really used anywhere, but may be useful for output validation.
    FeatureTable(const Json::Value& json, const char* pos);

    Json::Value getJson() const;

    void appendBinary(std::vector<char>& data) const;

    std::size_t bytes() const
    {
        return
            m_points.size() * 3 * sizeof(float) +
            m_normals.size() * 3 * sizeof(float) +
            m_colors.size() * 3 * sizeof(uint8_t);
    }

private:
    void summarizePoints() const;
    void summarizeColors() const;
    void summarizeNormals() const;

    std::vector<Point> m_points;
    std::vector<Point> m_normals;
    std::vector<Color> m_colors;
};

} // namespace cesium
} // namespace entwine


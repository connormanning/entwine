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
            const std::vector<Color>& colors);

    // Not really used anywhere, but may be useful for output validation.
    FeatureTable(const Json::Value& json, const char* pos);

    Json::Value getJson() const;

    std::vector<char> getBinary() const;

private:
    void summarizePoints() const;
    void summarizeColors() const;

    std::vector<Point> m_points;
    std::vector<Color> m_colors;
};

} // namespace cesium
} // namespace entwine


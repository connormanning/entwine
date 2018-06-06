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

#include <cstddef>
#include <string>

#include <json/json.h>

namespace entwine
{
namespace cesium
{

class Settings
{
public:
    Settings(
            std::size_t tilesetSplit,
            double geometricErrorDivisor,
            std::string coloring,
            bool truncate,
            std::vector<std::string> batchTableDimensions);

    Settings(const Json::Value& json);

    Json::Value toJson() const;

    std::size_t tilesetSplit() const { return m_tilesetSplit; }
    double geometricErrorDivisor() const { return m_geometricErrorDivisor; }
    const std::string& coloring() const { return m_coloring; }
    bool truncate() const { return m_truncate; }
    const std::vector<std::string>& batchTableDimensions() const { return m_batchTableDimensions; }

private:
    std::size_t m_tilesetSplit;
    double m_geometricErrorDivisor;
    std::string m_coloring;
    bool m_truncate;    // If true, color/intensity should be scaled to 8 bits.
    std::vector<std::string> m_batchTableDimensions;
};

} // namespace cesium
} // namespace entwine


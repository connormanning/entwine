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

#include <entwine/formats/cesium/settings.hpp>
#include <entwine/formats/cesium/tile.hpp>
#include <entwine/types/binary-point-table.hpp>

namespace entwine
{

class Metadata;
class Schema;

namespace cesium
{

class TileBuilder
{
public:
    TileBuilder(const Metadata& metadata, const TileInfo& info);

    void push(std::size_t tick, const Cell& cell);

    const std::map<std::size_t, TileData>& data() const
    {
        return m_data;
    }

private:
    std::size_t divisor() const
    {
        const auto& s(m_metadata.structure());

        std::size_t d(1 << s.nominalChunkDepth());

        if (m_info.depth() > s.sparseDepthBegin())
        {
            d <<= m_info.depth() - s.sparseDepthBegin();
        }

        return d;
    }

    uint8_t getByte(pdal::Dimension::Id id) const
    {
        if (!m_settings.truncate()) return m_pr.getFieldAs<uint8_t>(id);
        else return m_pr.getFieldAs<uint16_t>(id) >> 8;
    }

    const Metadata& m_metadata;
    const Schema& m_schema;
    const Settings& m_settings;
    const TileInfo& m_info;

    std::size_t m_divisor;
    bool m_hasColor;
    bool m_hasNormals;
    bool m_hasBatchTableDimensions;
    std::map<std::size_t, Color> m_tileColors;
    std::map<std::size_t, TileData> m_data;

    BinaryPointTable m_table;
    pdal::PointRef m_pr;
};

} // namespace cesium
} // namespace entwine


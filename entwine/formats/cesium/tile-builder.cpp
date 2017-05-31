/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/formats/cesium/tile-builder.hpp>

#include <cstdlib>

#include <entwine/types/metadata.hpp>
#include <entwine/types/schema.hpp>

namespace entwine
{
namespace cesium
{

TileBuilder::TileBuilder(const Metadata& metadata, const TileInfo& info)
    : m_metadata(metadata)
    , m_schema(m_metadata.schema())
    , m_settings(*m_metadata.cesiumSettings())
    , m_info(info)
    , m_divisor(divisor())
    , m_hasColor(false)
    , m_hasNormals(false)
    , m_table(m_schema)
    , m_pr(m_table, 0)
{
    m_hasColor =
        m_settings.coloring().size() ||
        m_schema.contains("Red") ||
        m_schema.contains("Green") ||
        m_schema.contains("Blue");

    m_hasNormals =
        m_schema.contains("NormalX") &&
        m_schema.contains("NormalY") &&
        m_schema.contains("NormalZ");

    for (const auto& p : info.ticks())
    {
        m_data.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(p.first),
                std::forward_as_tuple(p.second, m_hasColor, m_hasNormals));
    }

    if (m_settings.coloring() == "tile")
    {
        for (const auto& p : info.ticks())
        {
            m_tileColors[p.first] = Color(
                    std::rand() % 255,
                    std::rand() % 255,
                    std::rand() % 255);
        }
    }
}

void TileBuilder::push(std::size_t rawTick, const Cell& cell)
{
    const std::size_t tick(rawTick / m_divisor);
    auto& selected(m_data.at(tick));

    const auto delta(m_metadata.delta());

    for (const auto& single : cell)
    {
        m_table.setPoint(single);
        if (delta)

        {
            selected.points.emplace_back(
                    Point::unscale(
                        cell.point(),
                        delta->scale(),
                        delta->offset()));
        }
        else
        {
            selected.points.emplace_back(
                    cell.point().x,
                    cell.point().y,
                    cell.point().z);
        }

        if (m_hasColor)
        {
            if (m_settings.coloring().empty())
            {
                selected.colors.emplace_back(
                        getByte(pdal::Dimension::Id::Red),
                        getByte(pdal::Dimension::Id::Green),
                        getByte(pdal::Dimension::Id::Blue));
            }
            else if (m_settings.coloring() == "intensity")
            {
                const uint8_t in(getByte(pdal::Dimension::Id::Intensity));
                selected.colors.emplace_back(in, in, in);
            }
            else if (m_settings.coloring() == "tile")
            {
                selected.colors.emplace_back(m_tileColors.at(tick));
            }
        }

        if (m_hasNormals)
        {
            pdal::PointLayout *pl = m_table.layout();

            pdal::Dimension::Id normalXDim = pl->findDim("NormalX");
            pdal::Dimension::Id normalYDim = pl->findDim("NormalY");
            pdal::Dimension::Id normalZDim = pl->findDim("NormalZ");

            selected.normals.emplace_back(
                m_pr.getFieldAs<double>(normalXDim),
                m_pr.getFieldAs<double>(normalYDim),
                m_pr.getFieldAs<double>(normalZDim));
        }
    }
}

} // namespace cesium
} // namespace entwine


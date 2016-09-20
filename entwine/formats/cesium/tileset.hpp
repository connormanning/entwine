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

#include <entwine/formats/cesium/settings.hpp>
#include <entwine/formats/cesium/tile.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/types/metadata.hpp>

namespace entwine
{
namespace cesium
{

class Tileset
{
public:
    Tileset(const Metadata& metadata, const TileInfo& tileInfo)
        : m_metadata(metadata)
        , m_tileInfo(tileInfo)
    { }

    void writeTo(const arbiter::Endpoint& endpoint)
    {
        if (const auto cs = m_metadata.cesiumSettings())
        {
            const auto& s(m_metadata.structure());
            const Bounds& bounds(m_metadata.bounds());
            double baseGeometricError(
                    bounds.width() / cs->geometricErrorDivisor());

            if (s.baseDepthBegin() < 8)
            {
                for (std::size_t i(s.baseDepthBegin()); i < 8; ++i)
                {
                    baseGeometricError *= 2.0;
                }
            }
            else if (s.baseDepthBegin() > 8)
            {
                for (std::size_t i(s.baseDepthBegin()); i < 8; ++i)
                {
                    baseGeometricError /= 2.0;
                }
            }

            m_tileInfo.write(m_metadata, endpoint, baseGeometricError);
        }
        else
        {
            throw std::runtime_error(
                    "Cannot write tileset without cesium settings");
        }
    }

private:
    const Metadata& m_metadata;
    const TileInfo& m_tileInfo;
};

} // namespace cesium
} // namespace entwine


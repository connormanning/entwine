/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>
#include <vector>

#include <entwine/formats/cesium/tileset.hpp>
#include <entwine/types/key.hpp>

namespace entwine
{
namespace cesium
{

// This class represents a single PNTS file:
// https://git.io/f477J
class Pnts
{
    using Xyz = std::vector<float>;
    using Rgb = std::vector<uint8_t>;
    using Normals = std::vector<float>;

public:
    Pnts(const Tileset& tileset, const ChunkKey& ck);
    std::vector<char> build();

private:
    Xyz buildXyz(const Cell::PooledStack& cells) const;
    Rgb buildRgb(const Cell::PooledStack& cells) const;
    Normals buildNormals(const Cell::PooledStack& cells) const;

    std::vector<char> buildFile(
            const Xyz& xyz,
            const Rgb& rgb,
            const Normals& normals) const;

    const Tileset& m_tileset;
    const ChunkKey m_key;
    Point m_mid;

    std::size_t m_np = 0;
};

} // namespace cesium
} // namespace entwine


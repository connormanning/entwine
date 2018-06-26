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

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/key.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{
namespace cesium
{

enum class ColorType
{
    None,
    Rgb,
    Intensity,
    Tile
};

// This class is the entrypoint of a 3D Tiles tileset definition:
// https://github.com/AnalyticalGraphicsInc/3d-tiles#tilesetjson
class Tileset
{
    using HierarchyTree = std::map<Dxyz, uint64_t>;

public:
    Tileset(const Json::Value& config);

    void build() const;

    const arbiter::Endpoint& in() const { return m_in; }
    const arbiter::Endpoint& out() const { return m_out; }
    const arbiter::Endpoint& tmp() const { return m_tmp; }

    const Metadata& metadata() const { return m_metadata; }
    bool hasColor() const { return m_colorType != ColorType::None; }
    bool hasNormals() const { return m_hasNormals; }
    bool truncate() const { return m_truncate; }
    ColorType colorType() const { return m_colorType; }
    std::string colorString() const;
    double rootGeometricError() const { return m_rootGeometricError; }
    double geometricErrorAt(uint64_t depth) const
    {
        return m_rootGeometricError / std::pow(2.0, depth);
    }

    PointPool& pointPool() const { return m_pointPool; }
    Pool& threadPool() const { return m_threadPool; }

private:
    void build(const ChunkKey& ck) const;

    Json::Value build(
            uint64_t startDepth,
            const ChunkKey& ck,
            const HierarchyTree& hier) const;

    ColorType getColorType(const Json::Value& config) const;
    HierarchyTree getHierarchyTree(const ChunkKey& root) const;

    arbiter::Arbiter m_arbiter;
    const arbiter::Endpoint m_in;
    const arbiter::Endpoint m_out;
    const arbiter::Endpoint m_tmp;

    const Metadata m_metadata;
    const uint64_t m_hierarchyStep;
    const ColorType m_colorType;
    const bool m_truncate;
    const bool m_hasNormals;
    const double m_rootGeometricError;

    mutable PointPool m_pointPool;
    mutable Pool m_threadPool;
};

} // namespace cesium
} // namespace entwine


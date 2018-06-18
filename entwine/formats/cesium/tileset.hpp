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

namespace entwine
{
namespace cesium
{

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
    bool hasColor() const { return m_hasColor; }
    double rootGeometricError() const { return m_rootGeometricError; }
    double geometricErrorAt(uint64_t depth) const
    {
        double g(m_rootGeometricError);
        for (uint64_t d(0); d < depth; ++d) g /= 2.0;
        return g;
    }

    PointPool& pointPool() const { return m_pointPool; }

private:
    void buildSubtree(const ChunkKey& ck) const;

    Json::Value build(
            const HierarchyTree& h,
            const ChunkKey& ck) const;

    arbiter::Arbiter m_arbiter;
    const arbiter::Endpoint m_in;
    const arbiter::Endpoint m_out;
    const arbiter::Endpoint m_tmp;

    const Metadata m_metadata;
    const bool m_hasColor;
    const double m_rootGeometricError = 1000;

    mutable PointPool m_pointPool;
};

class Tile
{
public:
    Tile(const Tileset& tileset, const ChunkKey& ck);

    Json::Value toJson() const { return m_json; }

private:
    Json::Value toBox(Bounds in) const
    {
        if (const Delta* d = m_tileset.metadata().delta())
        {
            in = in.unscale(d->scale(), d->offset());
        }

        Json::Value box(in.mid().toJson());
        box.append(in.width()); box.append(0); box.append(0);
        box.append(0); box.append(in.depth()); box.append(0);
        box.append(0); box.append(0); box.append(in.height());
        return box;
    }

    const Tileset& m_tileset;
    const ChunkKey m_key;
    Json::Value m_json;
};

class Pnts
{
public:
    Pnts(const Tileset& tileset, const ChunkKey& ck);
    void build();

private:
    void buildXyz(const Cell::PooledStack& cells);
    void buildRgb(const Cell::PooledStack& cells);
    void write();

    const Tileset& m_tileset;
    const ChunkKey m_key;
    Point m_mid;

    std::size_t m_np = 0;
    std::vector<float> m_xyz;
    std::vector<uint8_t> m_rgb;
};

} // namespace cesium
} // namespace entwine


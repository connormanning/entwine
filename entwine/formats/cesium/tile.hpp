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

#include <entwine/formats/cesium/tileset.hpp>

namespace entwine
{
namespace cesium
{

// This class represents a the metadata for a single tile:
// https://github.com/AnalyticalGraphicsInc/3d-tiles#tile-metadata
class Tile
{
public:
    Tile(const Tileset& tileset, const ChunkKey& ck, bool external = false)
        : m_tileset(tileset)
        , m_json {
            { "boundingVolume", { { "box", toBox(ck.bounds()) } } },
            { "geometricError", m_tileset.geometricErrorAt(ck.depth()) },
            { "content", { { "url", external ?
                "tileset-" + ck.toString() + ".json" : ck.toString() + ".pnts"
            } } }
        }
    {
        if (!ck.depth()) m_json["refine"] = "ADD";
    }

    json get() const { return m_json; }

private:
    json toBox(Bounds in) const
    {
        return json {
            in.mid()[0],    in.mid()[1],    in.mid()[2],
            in.width(),     0,              0,
            0,              in.depth(),     0,
            0,              0,              in.height()
        };
    }

    const Tileset& m_tileset;
    json m_json;
};

inline void to_json(json& j, const Tile& t) { j = t.get(); }

} // namespace cesium
} // namespace entwine


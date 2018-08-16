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
    {
        m_json["boundingVolume"]["box"] = toBox(ck.bounds());
        m_json["geometricError"] = m_tileset.geometricErrorAt(ck.depth());
        m_json["content"]["url"] = external ?
            "tileset-" + ck.toString() + ".json" :
            ck.toString() + ".pnts";
        if (!ck.depth()) m_json["refine"] = "ADD";
    }

    Json::Value toJson() const { return m_json; }

private:
    Json::Value toBox(Bounds in) const
    {
        Json::Value box(in.mid().toJson());
        box.append(in.width()); box.append(0); box.append(0);
        box.append(0); box.append(in.depth()); box.append(0);
        box.append(0); box.append(0); box.append(in.height());
        return box;
    }

    const Tileset& m_tileset;
    Json::Value m_json;
};

} // namespace cesium
} // namespace entwine


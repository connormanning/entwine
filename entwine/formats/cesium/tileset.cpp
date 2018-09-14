/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/formats/cesium/pnts.hpp>
#include <entwine/formats/cesium/tile.hpp>
#include <entwine/formats/cesium/tileset.hpp>

namespace entwine
{
namespace cesium
{

namespace
{
    using DimId = pdal::Dimension::Id;
}

Tileset::Tileset(const Json::Value& config)
    : m_arbiter(config["arbiter"])
    , m_in(m_arbiter.getEndpoint(config["input"].asString()))
    , m_out(m_arbiter.getEndpoint(config["output"].asString()))
    , m_tmp(m_arbiter.getEndpoint(
                config.isMember("tmp") ?
                    config["tmp"].asString() : arbiter::fs::getTempPath()))
    , m_metadata(m_in)
    , m_hierarchyStep(m_metadata.hierarchyStep())
    , m_colorType(getColorType(config))
    , m_truncate(config["truncate"].asBool())
    , m_hasNormals(
            m_metadata.schema().contains(DimId::NormalX) &&
            m_metadata.schema().contains(DimId::NormalY) &&
            m_metadata.schema().contains(DimId::NormalZ))
    , m_rootGeometricError(
            m_metadata.boundsCubic().width() /
            (config.isMember("geometricErrorDivisor") ?
                config["geometricErrorDivisor"].asDouble() : 32.0))
    , m_threadPool(std::max<uint64_t>(4, config["threads"].asUInt64()))
{
    arbiter::fs::mkdirp(m_out.root());
    arbiter::fs::mkdirp(m_tmp.root());
}

std::string Tileset::colorString() const
{
    switch (m_colorType)
    {
        case ColorType::None:       return "none";
        case ColorType::Rgb:        return "rgb";
        case ColorType::Intensity:  return "intensity";
        case ColorType::Tile:       return "tile";
        default:                    return "unknown";
    }
}

ColorType Tileset::getColorType(const Json::Value& config) const
{
    if (config.isMember("colorType"))
    {
        const std::string s(config["colorType"].asString());
        if (s == "none")        return ColorType::None;
        if (s == "rgb")         return ColorType::Rgb;
        if (s == "intensity")   return ColorType::Intensity;
        if (s == "tile")        return ColorType::Tile;
        throw std::runtime_error("Invalid cesium colorType: " + s);
    }
    else if (
            m_metadata.schema().contains(DimId::Red) &&
            m_metadata.schema().contains(DimId::Green) &&
            m_metadata.schema().contains(DimId::Blue))
    {
        return ColorType::Rgb;
    }
    else if (m_metadata.schema().contains(DimId::Intensity))
    {
        return ColorType::Intensity;
    }

    return ColorType::None;
}

Tileset::HierarchyTree Tileset::getHierarchyTree(const ChunkKey& root) const
{
    HierarchyTree h;

    const Json::Value fetched(
            parse(m_in.get("h/" + root.get().toString() + ".json")));

    for (const std::string& key : fetched.getMemberNames())
    {
        h[Dxyz(key)] = fetched[key].asUInt64();
    }

    return h;
}

void Tileset::build() const
{
    build(ChunkKey(m_metadata));
    m_threadPool.join();
}

void Tileset::build(const ChunkKey& ck) const
{
    const HierarchyTree hier(getHierarchyTree(ck));

    Json::Value json;
    json["asset"]["version"] = "0.0";
    json["geometricError"] = m_rootGeometricError;
    json["root"] = build(ck.depth(), ck, hier);

    if (!ck.depth())
    {
        m_out.put("tileset.json", json.toStyledString());
    }
    else
    {
        m_out.put("tileset-" + ck.toString() + ".json", toFastString(json));
    }
}

Json::Value Tileset::build(
        uint64_t startDepth,
        const ChunkKey& ck,
        const HierarchyTree& hier) const
{
    uint64_t n(hier.count(ck.get()) ? hier.at(ck.get()) : 0);
    if (!n) return Json::nullValue;

    const bool leaf(
            m_hierarchyStep &&
            ck.depth() != startDepth &&
            ck.depth() % m_hierarchyStep == 0);

    if (leaf)
    {
        // Start a new subtree for this node.
        build(ck);

        // Write the pointer node to that external tileset.
        Tile tile(*this, ck, true);
        const Json::Value json(tile.toJson());
        return json;
    }

    m_threadPool.add([this, ck]()
    {
        Pnts pnts(*this, ck);
        m_out.put(ck.get().toString() + ".pnts", pnts.build());
    });

    Tile tile(*this, ck);
    Json::Value json(tile.toJson());

    for (std::size_t i(0); i < 8; ++i)
    {
        const auto child(build(startDepth, ck.getStep(toDir(i)), hier));
        if (!child.isNull())
        {
            json["children"].append(child);
        }
    }

    return json;
}

} // namespace cesium
} // namespace entwine


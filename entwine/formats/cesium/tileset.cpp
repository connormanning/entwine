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

Tileset::Tileset(const json& config)
    : m_arbiter(config.value("arbiter", json()).dump())
    , m_in(m_arbiter.getEndpoint(config.at("input").get<std::string>()))
    , m_out(m_arbiter.getEndpoint(config.at("output").get<std::string>()))
    , m_tmp(m_arbiter.getEndpoint(
                config.value("tmp", arbiter::getTempPath())))
    , m_metadata(m_in)
    , m_colorType(getColorType(config))
    , m_truncate(config.value("truncate", false))
    , m_hasNormals(
            m_metadata.schema().contains(DimId::NormalX) &&
            m_metadata.schema().contains(DimId::NormalY) &&
            m_metadata.schema().contains(DimId::NormalZ))
    , m_rootGeometricError(
            m_metadata.boundsCubic().width() /
                config.value("geometricErrorDivisor", 32.0))
    , m_threadPool(std::max<uint64_t>(4, config.value("threads", 4)))
{
    arbiter::mkdirp(m_out.root());
    arbiter::mkdirp(m_tmp.root());
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

ColorType Tileset::getColorType(const json& config) const
{
    if (config.count("colorType"))
    {
        const auto s(config.at("colorType").get<std::string>());
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
    const std::string file("ept-hierarchy/" + root.get().toString() + ".json");
    const auto fetched(json::parse(m_in.get(file)));

    for (const auto& p : fetched.items())
    {
        h[Dxyz(p.key())] = p.value().get<int64_t>();
    }

    return h;
}

void Tileset::build() const
{
    build(ChunkKey(m_metadata));
    m_threadPool.await();
}

void Tileset::build(const ChunkKey& ck) const
{
    const HierarchyTree hier(getHierarchyTree(ck));

    const json j {
        { "asset", { { "version", "1.0" } } },
        { "geometricError", m_rootGeometricError },
        { "root", build(ck.depth(), ck, hier) }
    };

    if (!ck.depth())
    {
        m_out.put("tileset.json", j.dump(2));
    }
    else
    {
        m_out.put("tileset-" + ck.toString() + ".json", j.dump());
    }
}

json Tileset::build(
        uint64_t startDepth,
        const ChunkKey& ck,
        const HierarchyTree& hier) const
{
    if (!hier.count(ck.get())) return json();

    if (hier.at(ck.get()) < 0)
    {
        // We're at a hierarchy leaf - start a new subtree for this node.
        build(ck);

        // Write the pointer node to that external tileset.
        return Tile(*this, ck, true);
    }

    m_threadPool.add([this, ck]()
    {
        Pnts pnts(*this, ck);
        m_out.put(ck.get().toString() + ".pnts", pnts.build());
    });

    json j(Tile(*this, ck));

    for (std::size_t i(0); i < 8; ++i)
    {
        const json child(build(startDepth, ck.getStep(toDir(i)), hier));
        if (!child.is_null()) j["children"].push_back(child);
    }

    return j;
}

} // namespace cesium
} // namespace entwine


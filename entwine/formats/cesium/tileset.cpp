/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/formats/cesium/tileset.hpp>
#include <entwine/io/io.hpp>
#include <entwine/types/binary-point-table.hpp>

namespace entwine
{
namespace cesium
{

Tileset::Tileset(const Json::Value& config)
    : m_arbiter(config["arbiter"])
    , m_in(m_arbiter.getEndpoint(config["input"].asString()))
    , m_out(m_arbiter.getEndpoint(config["output"].asString()))
    , m_tmp(m_arbiter.getEndpoint(
                config.isMember("tmp") ?
                    config["tmp"].asString() : arbiter::fs::getTempPath()))
    , m_metadata(m_in)
    , m_hierarchyStep(m_metadata.hierarchyStep())
    , m_hasColor(m_metadata.schema().contains("Red"))
    , m_rootGeometricError(
            m_metadata.boundsNativeCubic().width() /
            (config.isMember("geometricErrorDivisor") ?
                config["geometricErrorDivisor"].asDouble() : 32.0))
    , m_pointPool(m_metadata.schema(), m_metadata.delta())
    , m_threadPool(std::max<uint64_t>(4, config["threads"].asUInt64()))
{
    arbiter::fs::mkdirp(m_out.root());
    arbiter::fs::mkdirp(m_tmp.root());
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

Tile::Tile(const Tileset& tileset, const ChunkKey& ck, bool external)
    : m_tileset(tileset)
{
    m_json["boundingVolume"]["box"] = toBox(ck.bounds());
    m_json["geometricError"] = m_tileset.geometricErrorAt(ck.depth());
    m_json["content"]["url"] = external ?
        "tileset-" + ck.toString() + ".json" :
        ck.toString() + ".pnts";
    if (!ck.depth()) m_json["refine"] = "ADD";
}

Pnts::Pnts(const Tileset& tileset, const ChunkKey& ck)
    : m_tileset(tileset)
    , m_key(ck)
{
    m_mid = m_key.bounds().mid();

    if (const Delta* d = m_tileset.metadata().delta())
    {
        m_mid = Point::unscale(m_mid, d->scale(), d->offset());
    }
}

std::vector<char> Pnts::build()
{
    const auto data = m_tileset.metadata().dataIo().read(
            m_tileset.in(),
            m_tileset.tmp(),
            m_tileset.pointPool(),
            m_key.get().toString());

    m_np = data.size();

    const auto xyz(buildXyz(data));
    const auto rgb(buildRgb(data));
    return build(xyz, rgb);
}

Pnts::Xyz Pnts::buildXyz(const Cell::PooledStack& cells) const
{
    Xyz v;
    v.reserve(m_np * 3);

    Scale scale(1);
    Offset offset(0);

    if (const Delta* d = m_tileset.metadata().delta())
    {
        scale = d->scale();
        offset = d->offset();
    }

    for (const auto& cell : cells)
    {
        const Point p(Point::unscale(cell.point(), scale, offset));
        v.push_back(p.x - m_mid.x);
        v.push_back(p.y - m_mid.y);
        v.push_back(p.z - m_mid.z);
    }

    return v;
}

Pnts::Rgb Pnts::buildRgb(const Cell::PooledStack& cells) const
{
    Rgb v;
    v.reserve(m_np * 3);

    using DimId = pdal::Dimension::Id;
    BinaryPointTable table(m_tileset.metadata().schema());

    for (const auto& cell : cells)
    {
        table.setPoint(cell.uniqueData());
        v.push_back(table.ref().getFieldAs<uint8_t>(DimId::Red));
        v.push_back(table.ref().getFieldAs<uint8_t>(DimId::Green));
        v.push_back(table.ref().getFieldAs<uint8_t>(DimId::Blue));
    }

    return v;
}

std::vector<char> Pnts::build(const Xyz& xyz, const Rgb& rgb) const
{
    Json::Value featureTable;
    featureTable["POINTS_LENGTH"] = static_cast<Json::UInt64>(m_np);
    featureTable["POSITION"]["byteOffset"] = 0;
    featureTable["RTC_CENTER"] = m_mid.toJson();

    if (m_tileset.hasColor())
    {
        featureTable["RGB"]["byteOffset"] = static_cast<Json::UInt64>(
                xyz.size() * sizeof(float));
    }

    std::string featureString = toFastString(featureTable);
    while (featureString.size() % 8) featureString += ' ';

    const uint64_t headerSize(28);
    const uint64_t binaryBytes = xyz.size() * (sizeof(float)) + rgb.size();
    const uint64_t totalBytes = headerSize + featureString.size() + binaryBytes;

    std::vector<char> header;
    header.reserve(headerSize);

    auto push([&header](uint32_t v)
    {
        header.insert(
                header.end(),
                reinterpret_cast<char*>(&v),
                reinterpret_cast<char*>(&v + 1));
    });

    const std::string magic("pnts");
    header.insert(header.end(), magic.begin(), magic.end());
    push(1);                    // Version.
    push(totalBytes);           // ByteLength.
    push(featureString.size()); // FeatureTableJsonByteLength.
    push(binaryBytes);          // FeatureTableBinaryByteLength.
    push(0);                    // BatchTableJsonByteLength.
    push(0);                    // BatchTableBinaryByteLength.
    assert(header.size() == headerSize);

    std::vector<char> pnts;
    pnts.reserve(totalBytes);

    pnts.insert(pnts.end(), header.begin(), header.end());
    pnts.insert(pnts.end(), featureString.begin(), featureString.end());
    pnts.insert(
            pnts.end(),
            reinterpret_cast<const char*>(xyz.data()),
            reinterpret_cast<const char*>(xyz.data() + xyz.size()));
    pnts.insert(
            pnts.end(),
            reinterpret_cast<const char*>(rgb.data()),
            reinterpret_cast<const char*>(rgb.data() + rgb.size()));

    return pnts;
}

} // namespace cesium
} // namespace entwine


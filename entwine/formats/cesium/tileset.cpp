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
    , m_hasColor(m_metadata.schema().contains("Red"))
    , m_pointPool(m_metadata.schema(), m_metadata.delta())
{
    arbiter::fs::mkdirp(m_out.root());
    arbiter::fs::mkdirp(m_tmp.root());
}

void Tileset::build() const
{
    HierarchyTree h;

    const Json::Value hier(parse(m_in.get("h/0-0-0-0.json")));
    for (const std::string& key : hier.getMemberNames())
    {
        h[Dxyz(key)] = hier[key].asUInt64();
    }

    Json::Value json;
    json["asset"]["version"] = "0.0";
    json["geometricError"] = 10000;
    json["root"] = build(h, ChunkKey(m_metadata));

    m_out.put("tileset.json", json.toStyledString());
}

Json::Value Tileset::build(
        const HierarchyTree& h,
        const ChunkKey& ck) const
{
    uint64_t n(h.count(ck.get()) ? h.at(ck.get()) : 0);
    if (!n) return Json::nullValue;

    Pnts pnts(*this, ck);
    pnts.build();

    Tile tile(*this, ck);
    Json::Value json(tile.toJson());

    for (std::size_t i(0); i < 8; ++i)
    {
        const auto child(build(h, ck.getStep(toDir(i))));
        if (!child.isNull())
        {
            json["children"].append(child);
        }
    }

    return json;
}

Tile::Tile(const Tileset& tileset, const ChunkKey& ck)
    : m_tileset(tileset)
    , m_key(ck)
{
    m_json["boundingVolume"]["box"] = toBox(ck.bounds());
    m_json["content"]["url"] = ck.toString() + ".pnts";
    m_json["geometricError"] = m_tileset.geometricErrorAt(ck.depth());
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

void Pnts::build()
{
    const auto data = m_tileset.metadata().dataIo().read(
            m_tileset.in(),
            m_tileset.tmp(),
            m_tileset.pointPool(),
            m_key.get().toString());

    m_np = data.size();

    buildXyz(data);
    if (m_tileset.hasColor()) buildRgb(data);
    write();
}

void Pnts::buildXyz(const Cell::PooledStack& cells)
{
    m_xyz.reserve(m_np * 3);

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
        m_xyz.push_back(p.x - m_mid.x);
        m_xyz.push_back(p.y - m_mid.y);
        m_xyz.push_back(p.z - m_mid.z);
    }
}

void Pnts::buildRgb(const Cell::PooledStack& cells)
{
    assert(m_tileset.hasColor());
    m_rgb.reserve(m_np * 3);

    using DimId = pdal::Dimension::Id;
    BinaryPointTable table(m_tileset.metadata().schema());

    for (const auto& cell : cells)
    {
        table.setPoint(cell.uniqueData());
        m_rgb.push_back(table.ref().getFieldAs<uint8_t>(DimId::Red));
        m_rgb.push_back(table.ref().getFieldAs<uint8_t>(DimId::Green));
        m_rgb.push_back(table.ref().getFieldAs<uint8_t>(DimId::Blue));
    }
}

void Pnts::write()
{
    Json::Value featureTable;
    featureTable["POINTS_LENGTH"] = static_cast<Json::UInt64>(m_np);
    featureTable["POSITION"]["byteOffset"] = 0;
    featureTable["RTC_CENTER"] = m_mid.toJson();

    if (m_tileset.hasColor())
    {
        featureTable["RGB"]["byteOffset"] = static_cast<Json::UInt64>(
                m_xyz.size() * sizeof(float));
    }

    std::string featureString = toFastString(featureTable);
    while (featureString.size() % 8) featureString += ' ';

    const uint64_t headerSize(28);
    const uint64_t binaryBytes = m_xyz.size() * (sizeof(float)) + m_rgb.size();
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
            reinterpret_cast<const char*>(m_xyz.data()),
            reinterpret_cast<const char*>(m_xyz.data() + m_xyz.size()));
    pnts.insert(
            pnts.end(),
            reinterpret_cast<const char*>(m_rgb.data()),
            reinterpret_cast<const char*>(m_rgb.data() + m_rgb.size()));

    m_xyz.clear();
    m_rgb.clear();

    m_tileset.out().put(m_key.get().toString() + ".pnts", pnts);
}

} // namespace cesium
} // namespace entwine


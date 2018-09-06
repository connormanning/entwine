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

#include <entwine/io/io.hpp>
#include <entwine/types/binary-point-table.hpp>

namespace entwine
{
namespace cesium
{

Pnts::Pnts(const Tileset& tileset, const ChunkKey& ck)
    : m_tileset(tileset)
    , m_key(ck)
{
    m_mid = m_key.bounds().mid();
}

std::vector<char> Pnts::build()
{
    VectorPointTable table(m_tileset.metadata().schema());
    /*
            m_tileset.metadata().dataIo().read(
                m_tileset.in(),
                m_tileset.tmp(),
                m_tileset.metadata().schema(),
                m_key.get().toString()));
                */

    m_np = table.size();

    const auto xyz(buildXyz(table));
    const auto rgb(buildRgb(table));
    const auto normals(buildNormals(table));
    return buildFile(xyz, rgb, normals);
}

Pnts::Xyz Pnts::buildXyz(const VectorPointTable& table) const
{
    Xyz xyz;
    /*
    xyz.reserve(m_np * 3);

    for (const auto& cell : cells)
    {
        const Point p(cell.point());
        xyz.push_back(p.x - m_mid.x);
        xyz.push_back(p.y - m_mid.y);
        xyz.push_back(p.z - m_mid.z);
    }
    */

    return xyz;
}

Pnts::Rgb Pnts::buildRgb(const VectorPointTable& table) const
{
    Rgb rgb;
    /*
    if (!m_tileset.hasColor()) return rgb;
    rgb.reserve(m_np * 3);

    using DimId = pdal::Dimension::Id;
    BinaryPointTable table(m_tileset.metadata().schema());

    auto getByte([this, &table](DimId id) -> uint8_t
    {
        if (!m_tileset.truncate()) return table.ref().getFieldAs<uint8_t>(id);
        else return table.ref().getFieldAs<uint16_t>(id) >> 8;
    });

    uint8_t r, g, b;

    if (m_tileset.colorType() == ColorType::Tile)
    {
        r = std::rand() % 256;
        g = std::rand() % 256;
        b = std::rand() % 256;
    }

    assert(m_tileset.colorType() != ColorType::None);
    for (const auto& cell : cells)
    {
        table.setPoint(cell.uniqueData());
        if (m_tileset.colorType() == ColorType::Rgb)
        {
            r = getByte(DimId::Red);
            g = getByte(DimId::Green);
            b = getByte(DimId::Blue);
        }
        else if (m_tileset.colorType() == ColorType::Intensity)
        {
            r = g = b = getByte(DimId::Intensity);
        }

        rgb.push_back(r);
        rgb.push_back(g);
        rgb.push_back(b);
    }
    */

    return rgb;
}

Pnts::Normals Pnts::buildNormals(const VectorPointTable& table) const
{
    Normals normals;
    /*
    if (!m_tileset.hasNormals()) return normals;
    normals.reserve(m_np * 3);

    using DimId = pdal::Dimension::Id;
    BinaryPointTable table(m_tileset.metadata().schema());

    for (const auto& cell : cells)
    {
        table.setPoint(cell.uniqueData());
        normals.push_back(table.ref().getFieldAs<float>(DimId::NormalX));
        normals.push_back(table.ref().getFieldAs<float>(DimId::NormalY));
        normals.push_back(table.ref().getFieldAs<float>(DimId::NormalZ));
    }
    */

    return normals;
}

std::vector<char> Pnts::buildFile(
        const Xyz& xyz,
        const Rgb& rgb,
        const Normals& normals) const
{
    Json::Value featureTable;
    featureTable["POINTS_LENGTH"] = static_cast<Json::UInt64>(m_np);
    featureTable["RTC_CENTER"] = m_mid.toJson();

    Json::UInt64 byteOffset(0);
    featureTable["POSITION"]["byteOffset"] = byteOffset;
    byteOffset += xyz.size() * sizeof(float);

    if (m_tileset.hasColor())
    {
        featureTable["RGB"]["byteOffset"] = byteOffset;
        byteOffset += rgb.size();
    }

    if (m_tileset.hasNormals())
    {
        featureTable["NORMAL"]["byteOffset"] = byteOffset;
        byteOffset += normals.size() * sizeof(float);
    }

    std::string featureString = toFastString(featureTable);
    while (featureString.size() % 8) featureString += ' ';

    const uint64_t headerSize(28);
    const uint64_t binaryBytes =
        xyz.size() * sizeof(float) +
        rgb.size() +
        normals.size() * sizeof(float);
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
    pnts.insert(
            pnts.end(),
            reinterpret_cast<const char*>(normals.data()),
            reinterpret_cast<const char*>(normals.data() + normals.size()));

    return pnts;
}

} // namespace cesium
} // namespace entwine


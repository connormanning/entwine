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
    , m_mid(m_key.bounds().mid())
{ }

std::vector<char> Pnts::build()
{
    VectorPointTable table(m_tileset.metadata().schema());
    table.setProcess([this, &table]()
    {
        m_np += table.size();
        buildXyz(table);
        buildRgb(table);
        buildNormals(table);
    });

    m_tileset.metadata().dataIo().read(
            m_tileset.in(),
            m_tileset.tmp(),
            m_key.get().toString(),
            table);

    return buildFile();
}

void Pnts::buildXyz(VectorPointTable& table)
{
    m_xyz.reserve(m_xyz.size() + table.size() * 3);

    for (const auto& pr : table)
    {
        m_xyz.push_back(pr.getFieldAs<double>(DimId::X) - m_mid.x);
        m_xyz.push_back(pr.getFieldAs<double>(DimId::Y) - m_mid.y);
        m_xyz.push_back(pr.getFieldAs<double>(DimId::Z) - m_mid.z);
    }
}

void Pnts::buildRgb(VectorPointTable& table)
{
    if (!m_tileset.hasColor()) return;
    m_rgb.reserve(m_rgb.size() + table.size() * 3);

    auto getByte([this](const pdal::PointRef& pr, DimId id) -> uint8_t
    {
        if (!m_tileset.truncate()) return pr.getFieldAs<uint8_t>(id);
        else return pr.getFieldAs<uint16_t>(id) >> 8;
    });

    uint8_t r, g, b;

    if (m_tileset.colorType() == ColorType::Tile)
    {
        r = std::rand() % 256;
        g = std::rand() % 256;
        b = std::rand() % 256;
    }

    assert(m_tileset.colorType() != ColorType::None);
    for (const auto& pr : table)
    {
        if (m_tileset.colorType() == ColorType::Rgb)
        {
            r = getByte(pr, DimId::Red);
            g = getByte(pr, DimId::Green);
            b = getByte(pr, DimId::Blue);
        }
        else if (m_tileset.colorType() == ColorType::Intensity)
        {
            r = g = b = getByte(pr, DimId::Intensity);
        }

        m_rgb.push_back(r);
        m_rgb.push_back(g);
        m_rgb.push_back(b);
    }
}

void Pnts::buildNormals(VectorPointTable& table)
{
    if (!m_tileset.hasNormals()) return;
    m_normals.reserve(m_normals.size() + table.size() * 3);

    for (const auto& pr : table)
    {
        m_normals.push_back(pr.getFieldAs<float>(DimId::NormalX));
        m_normals.push_back(pr.getFieldAs<float>(DimId::NormalY));
        m_normals.push_back(pr.getFieldAs<float>(DimId::NormalZ));
    }
}

std::vector<char> Pnts::buildFile() const
{
    Json::Value featureTable;
    featureTable["POINTS_LENGTH"] = static_cast<Json::UInt64>(m_np);
    featureTable["RTC_CENTER"] = m_mid.toJson();

    Json::UInt64 byteOffset(0);
    featureTable["POSITION"]["byteOffset"] = byteOffset;
    byteOffset += m_xyz.size() * sizeof(float);

    if (m_tileset.hasColor())
    {
        featureTable["RGB"]["byteOffset"] = byteOffset;
        byteOffset += m_rgb.size();
    }

    if (m_tileset.hasNormals())
    {
        featureTable["NORMAL"]["byteOffset"] = byteOffset;
        byteOffset += m_normals.size() * sizeof(float);
    }

    std::string featureString = toFastString(featureTable);
    while (featureString.size() % 8) featureString += ' ';

    const uint64_t headerSize(28);
    const uint64_t binaryBytes =
        m_xyz.size() * sizeof(float) +
        m_rgb.size() +
        m_normals.size() * sizeof(float);
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
    pnts.insert(
            pnts.end(),
            reinterpret_cast<const char*>(m_normals.data()),
            reinterpret_cast<const char*>(m_normals.data() + m_normals.size()));

    return pnts;
}

} // namespace cesium
} // namespace entwine


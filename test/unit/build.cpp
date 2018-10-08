#include "gtest/gtest.h"
#include "config.hpp"
#include "verify.hpp"

#include <entwine/builder/builder.hpp>
#include <entwine/builder/merger.hpp>
#include <entwine/builder/scan.hpp>

using namespace entwine;
using DimId = pdal::Dimension::Id;

namespace
{
    const arbiter::Arbiter a;
    const Verify v;
}

TEST(build, basic)
{
    const std::string outPath(test::dataPath() + "out/ellipsoid/");
    const std::string metaPath(outPath + "ept-metadata/");

    Config c;
    c["input"] = test::dataPath() + "ellipsoid-multi/";
    c["output"] = outPath;
    c["force"] = true;
    c["ticks"] = static_cast<Json::UInt64>(v.ticks());
    c["hierarchyStep"] = static_cast<Json::UInt64>(v.hierarchyStep());

    Builder(c).go();

    const auto info(parse(a.get(outPath + "ept.json")));

    const Bounds bounds(info["bounds"]);
    const Bounds boundsConforming(info["boundsConforming"]);
    EXPECT_TRUE(bounds.isCubic());
    EXPECT_TRUE(bounds.contains(boundsConforming));
    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(boundsConforming[i], v.bounds()[i], 2.0) << "At: " << i <<
            "\n" << boundsConforming << "\n!=\n" << v.bounds() << std::endl;
    }

    const auto dataType(info["dataType"].asString());
    EXPECT_EQ(dataType, "laszip");

    const auto hierarchyType(info["hierarchyType"].asString());
    const auto hierarchyStep(info["hierarchyStep"].asUInt64());
    EXPECT_EQ(hierarchyType, "json");
    EXPECT_EQ(hierarchyStep, 2u);

    const auto numPoints(info["numPoints"].asUInt64());
    EXPECT_EQ(numPoints, v.numPoints());

    const Schema schema(info["schema"]);
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info["ticks"].asUInt64(), v.ticks());
    EXPECT_EQ(info["hierarchyStep"].asUInt64(), v.hierarchyStep());

    const auto files(parse(a.get(outPath + "ept-files.json")));
    ASSERT_EQ(files.size(), 9u);

    for (Json::ArrayIndex o(0); o < 8; ++o)
    {
        const auto path(metaPath + std::to_string(o) + ".json");
        const auto meta(parse(a.get(path)));
        ASSERT_FALSE(meta.isNull());
        ASSERT_EQ(files[o]["pointStats"]["outOfBounds"].asUInt64(), 0u) <<
            files[o];
    }

    const auto path(metaPath + "8.json");
    const auto meta(parse(a.get(path)));
    EXPECT_TRUE(meta.isNull()) << meta;
}

TEST(build, fromScan)
{
    const std::string scanPath(test::dataPath() + "out/prebuild-scan/");

    {
        const std::string dataPath(test::dataPath() + "ellipsoid-multi");

        Json::Value c;
        c["input"] = dataPath;
        c["output"] = scanPath;
        Scan(c).go();
    }

    const std::string outPath(test::dataPath() + "out/from-scan/");
    const std::string metaPath(outPath + "ept-metadata/");

    Config c;
    c["input"] = scanPath + "ept-scan.json";
    c["output"] = outPath;
    c["force"] = true;
    c["ticks"] = static_cast<Json::UInt64>(v.ticks());
    c["hierarchyStep"] = static_cast<Json::UInt64>(v.hierarchyStep());

    Builder(c).go();

    const auto info(parse(a.get(outPath + "ept.json")));

    const Bounds bounds(info["bounds"]);
    const Bounds boundsConforming(info["boundsConforming"]);
    EXPECT_TRUE(bounds.isCubic());
    EXPECT_TRUE(bounds.contains(boundsConforming));
    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(boundsConforming[i], v.bounds()[i], 2.0) << "At: " << i <<
            "\n" << boundsConforming << "\n!=\n" << v.bounds() << std::endl;
    }

    const auto dataType(info["dataType"].asString());
    EXPECT_EQ(dataType, "laszip");

    const auto hierarchyType(info["hierarchyType"].asString());
    const auto hierarchyStep(info["hierarchyStep"].asUInt64());
    EXPECT_EQ(hierarchyType, "json");
    EXPECT_EQ(hierarchyStep, 2u);

    const auto numPoints(info["numPoints"].asUInt64());
    EXPECT_EQ(numPoints, v.numPoints());

    const Schema schema(info["schema"]);
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info["ticks"].asUInt64(), v.ticks());
    EXPECT_EQ(info["hierarchyStep"].asUInt64(), v.hierarchyStep());

    const auto files(parse(a.get(outPath + "ept-files.json")));
    ASSERT_EQ(files.size(), 9u);

    for (Json::ArrayIndex o(0); o < 8; ++o)
    {
        const auto path(metaPath + std::to_string(o) + ".json");
        const auto meta(parse(a.get(path)));
        ASSERT_FALSE(meta.isNull());
        ASSERT_EQ(files[o]["pointStats"]["outOfBounds"].asUInt64(), 0u) <<
            files[o];
    }

    const auto path(metaPath + "8.json");
    const auto meta(parse(a.get(path)));
    EXPECT_TRUE(meta.isNull()) << meta;
}

TEST(build, subset)
{
    const std::string outPath(test::dataPath() + "out/subset/");
    const std::string metaPath(outPath + "ept-metadata/");

    for (Json::UInt64 i(0); i < 4u; ++i)
    {
        Config c;
        c["input"] = test::dataPath() + "ellipsoid-multi/";
        c["output"] = outPath;
        c["force"] = true;
        c["ticks"] = static_cast<Json::UInt64>(v.ticks());
        c["hierarchyStep"] = static_cast<Json::UInt64>(v.hierarchyStep());
        c["subset"]["id"] = i + 1u;
        c["subset"]["of"] = 4u;

        Builder(c).go();
    }

    {
        Config c;
        c["output"] = outPath;

        Merger(c).go();
    }

    const auto info(parse(a.get(outPath + "ept.json")));

    const Bounds bounds(info["bounds"]);
    const Bounds boundsConforming(info["boundsConforming"]);
    EXPECT_TRUE(bounds.isCubic());
    EXPECT_TRUE(bounds.contains(boundsConforming));
    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(boundsConforming[i], v.bounds()[i], 2.0) << "At: " << i <<
            "\n" << boundsConforming << "\n!=\n" << v.bounds() << std::endl;
    }

    const auto dataType(info["dataType"].asString());
    EXPECT_EQ(dataType, "laszip");

    const auto hierarchyType(info["hierarchyType"].asString());
    const auto hierarchyStep(info["hierarchyStep"].asUInt64());
    EXPECT_EQ(hierarchyType, "json");
    EXPECT_EQ(hierarchyStep, 2u);

    const auto numPoints(info["numPoints"].asUInt64());
    EXPECT_EQ(numPoints, v.numPoints());

    const Schema schema(info["schema"]);
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info["ticks"].asUInt64(), v.ticks());
    EXPECT_EQ(info["hierarchyStep"].asUInt64(), v.hierarchyStep());

    const auto files(parse(a.get(outPath + "ept-files.json")));
    ASSERT_EQ(files.size(), 9u);

    for (Json::ArrayIndex o(0); o < 8u; ++o)
    {
        const auto path(metaPath + std::to_string(o) + ".json");
        const auto meta(parse(a.get(path)));
        ASSERT_FALSE(meta.isNull());
        ASSERT_EQ(files[o]["pointStats"]["outOfBounds"].asUInt64(), 0u) <<
            files[o];
    }

    const auto path(metaPath + "8.json");
    const auto meta(parse(a.get(path)));
    EXPECT_TRUE(meta.isNull()) << meta;
}


TEST(build, subsetFromScan)
{
    const std::string scanPath(test::dataPath() + "out/prebuild-scan/");

    {
        const std::string dataPath(test::dataPath() + "ellipsoid-multi");

        Json::Value c;
        c["input"] = dataPath;
        c["output"] = scanPath;
        Scan(c).go();
    }

    const std::string outPath(test::dataPath() + "out/from-scan-subset/");
    const std::string metaPath(outPath + "ept-metadata/");

    for (Json::UInt64 i(0); i < 4u; ++i)
    {
        Config c;
        c["input"] = scanPath + "ept-scan.json";
        c["output"] = outPath;
        c["force"] = true;
        c["ticks"] = static_cast<Json::UInt64>(v.ticks());
        c["hierarchyStep"] = static_cast<Json::UInt64>(v.hierarchyStep());
        c["subset"]["id"] = i + 1u;
        c["subset"]["of"] = 4u;

        Builder(c).go();
    }

    {
        Config c;
        c["output"] = outPath;

        Merger(c).go();
    }

    const auto info(parse(a.get(outPath + "ept.json")));

    const Bounds bounds(info["bounds"]);
    const Bounds boundsConforming(info["boundsConforming"]);
    EXPECT_TRUE(bounds.isCubic());
    EXPECT_TRUE(bounds.contains(boundsConforming));
    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(boundsConforming[i], v.bounds()[i], 2.0) << "At: " << i <<
            "\n" << boundsConforming << "\n!=\n" << v.bounds() << std::endl;
    }

    const auto dataType(info["dataType"].asString());
    EXPECT_EQ(dataType, "laszip");

    const auto hierarchyType(info["hierarchyType"].asString());
    const auto hierarchyStep(info["hierarchyStep"].asUInt64());
    EXPECT_EQ(hierarchyType, "json");
    EXPECT_EQ(hierarchyStep, 2u);

    const auto numPoints(info["numPoints"].asUInt64());
    EXPECT_EQ(numPoints, v.numPoints());

    const Schema schema(info["schema"]);
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info["ticks"].asUInt64(), v.ticks());
    EXPECT_EQ(info["hierarchyStep"].asUInt64(), v.hierarchyStep());

    const auto files(parse(a.get(outPath + "ept-files.json")));
    ASSERT_EQ(files.size(), 9u);

    for (Json::ArrayIndex o(0); o < 8; ++o)
    {
        const auto path(metaPath + std::to_string(o) + ".json");
        const auto meta(parse(a.get(path)));
        ASSERT_FALSE(meta.isNull());
        ASSERT_EQ(files[o]["pointStats"]["outOfBounds"].asUInt64(), 0u) <<
            files[o];
    }

    const auto path(metaPath + "8.json");
    const auto meta(parse(a.get(path)));
    EXPECT_TRUE(meta.isNull()) << meta;
}

TEST(build, reprojected)
{
    const std::string outPath(test::dataPath() + "out/ellipsoid-re/");
    const std::string metaPath(outPath + "ept-metadata/");

    Config c;
    c["input"] = test::dataPath() + "ellipsoid-multi/";
    c["output"] = outPath;
    c["reprojection"]["out"] = "EPSG:26918";
    c["force"] = true;
    c["ticks"] = static_cast<Json::UInt64>(v.ticks());
    c["hierarchyStep"] = static_cast<Json::UInt64>(v.hierarchyStep());

    Builder(c).go();

    const auto info(parse(a.get(outPath + "ept.json")));

    const Bounds bounds(info["bounds"]);
    const Bounds boundsConforming(info["boundsConforming"]);
    EXPECT_TRUE(bounds.isCubic());
    EXPECT_TRUE(bounds.contains(boundsConforming));
    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(boundsConforming[i], v.boundsUtm()[i], 2.0) <<
            "At: " << i <<
            "\n" << boundsConforming << "\n!=\n" << v.boundsUtm() << std::endl;
    }

    const auto dataType(info["dataType"].asString());
    EXPECT_EQ(dataType, "laszip");

    const auto hierarchyType(info["hierarchyType"].asString());
    const auto hierarchyStep(info["hierarchyStep"].asUInt64());
    EXPECT_EQ(hierarchyType, "json");
    EXPECT_EQ(hierarchyStep, 2u);

    const auto numPoints(info["numPoints"].asUInt64());
    EXPECT_EQ(numPoints, v.numPoints());

    const Schema schema(info["schema"]);
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info["ticks"].asUInt64(), v.ticks());
    EXPECT_EQ(info["hierarchyStep"].asUInt64(), v.hierarchyStep());

    const auto files(parse(a.get(outPath + "ept-files.json")));
    ASSERT_EQ(files.size(), 9u);

    for (Json::ArrayIndex o(0); o < 8; ++o)
    {
        const auto path(metaPath + std::to_string(o) + ".json");
        const auto meta(parse(a.get(path)));
        ASSERT_FALSE(meta.isNull());
        ASSERT_EQ(files[o]["pointStats"]["outOfBounds"].asUInt64(), 0u) <<
            files[o];
    }

    const auto path(metaPath + "8.json");
    const auto meta(parse(a.get(path)));
    EXPECT_TRUE(meta.isNull()) << meta;
}


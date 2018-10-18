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
    const std::string metaPath(outPath + "ept-sources/");

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
    EXPECT_EQ(hierarchyType, "json");

    const auto points(info["points"].asUInt64());
    EXPECT_EQ(points, v.points());

    const Schema schema(info["schema"]);
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info["ticks"].asUInt64(), v.ticks());

    EXPECT_EQ(parse(a.get(outPath + "ept-sources/list.json")).size(), 8u);

    for (Json::ArrayIndex o(0); o < 8; ++o)
    {
        const auto path(metaPath + std::to_string(o) + ".json");
        const auto meta(parse(a.get(path)));
        ASSERT_FALSE(meta.isNull());
        ASSERT_GT(meta["points"].asUInt64(), 0u);
    }
}

TEST(build, continued)
{
    const std::string outPath(test::dataPath() + "out/ellipsoid/");
    const std::string metaPath(outPath + "ept-sources/");

    {
        Config c;
        c["input"] = test::dataPath() + "ellipsoid-multi/";
        c["output"] = outPath;
        c["force"] = true;
        c["ticks"] = static_cast<Json::UInt64>(v.ticks());
        c["hierarchyStep"] = static_cast<Json::UInt64>(v.hierarchyStep());
        c["run"] = 4;

        Builder(c).go();
    }

    {
        Config c;
        c["output"] = outPath;

        Builder(c).go();
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
    EXPECT_EQ(hierarchyType, "json");

    const auto points(info["points"].asUInt64());
    EXPECT_EQ(points, v.points());

    const Schema schema(info["schema"]);
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info["ticks"].asUInt64(), v.ticks());

    EXPECT_EQ(parse(a.get(outPath + "ept-sources/list.json")).size(), 8u);

    for (Json::ArrayIndex o(0); o < 8; ++o)
    {
        const auto path(metaPath + std::to_string(o) + ".json");
        const auto meta(parse(a.get(path)));
        ASSERT_FALSE(meta.isNull());
        ASSERT_GT(meta["points"].asUInt64(), 0u);
    }
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
    const std::string metaPath(outPath + "ept-sources/");

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
    EXPECT_EQ(hierarchyType, "json");

    const auto points(info["points"].asUInt64());
    EXPECT_EQ(points, v.points());

    const Schema schema(info["schema"]);
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info["ticks"].asUInt64(), v.ticks());

    EXPECT_EQ(parse(a.get(outPath + "ept-sources/list.json")).size(), 8u);

    for (Json::ArrayIndex o(0); o < 8; ++o)
    {
        const auto path(metaPath + std::to_string(o) + ".json");
        const auto meta(parse(a.get(path)));
        ASSERT_FALSE(meta.isNull());
        ASSERT_GT(meta["points"].asUInt64(), 0u);
    }
}

TEST(build, subset)
{
    const std::string outPath(test::dataPath() + "out/subset/");
    const std::string metaPath(outPath + "ept-sources/");

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
    EXPECT_EQ(hierarchyType, "json");

    const auto points(info["points"].asUInt64());
    EXPECT_EQ(points, v.points());

    const Schema schema(info["schema"]);
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info["ticks"].asUInt64(), v.ticks());

    EXPECT_EQ(parse(a.get(outPath + "ept-sources/list.json")).size(), 8u);

    for (Json::ArrayIndex o(0); o < 8; ++o)
    {
        const auto path(metaPath + std::to_string(o) + ".json");
        const auto meta(parse(a.get(path)));
        ASSERT_FALSE(meta.isNull());
        ASSERT_GT(meta["points"].asUInt64(), 0u);
    }
}

TEST(build, invalidSubset)
{
    const std::string outPath(test::dataPath() + "out/subset/");

    Config c;
    c["input"] = test::dataPath() + "ellipsoid-multi/";
    c["output"] = outPath;
    c["force"] = true;
    c["ticks"] = static_cast<Json::UInt64>(v.ticks());
    c["hierarchyStep"] = static_cast<Json::UInt64>(v.hierarchyStep());
    c["subset"]["id"] = 1;

    // Invalid subset range - must be more than one subset.
    c["subset"]["of"] = 1;
    EXPECT_ANY_THROW(Builder(c).go());

    // Invalid subset range - must be a perfect square.
    c["subset"]["of"] = 8;
    EXPECT_ANY_THROW(Builder(c).go());

    // Invalid subset range - must be a power of 2.
    c["subset"]["of"] = 9;
    EXPECT_ANY_THROW(Builder(c).go());

    // Invalid subset range.
    c["subset"]["of"] = 3320;
    EXPECT_ANY_THROW(Builder(c).go());

    c["subset"]["of"] = 4;

    // Invalid subset ID - must be 1-based.
    c["subset"]["id"] = 0;
    EXPECT_ANY_THROW(Builder(c).go());

    // Invalid subset ID - must be less than or equal to total subsets.
    c["subset"]["id"] = 5;
    EXPECT_ANY_THROW(Builder(c).go());
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
    const std::string metaPath(outPath + "ept-sources/");

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
    EXPECT_EQ(hierarchyType, "json");

    const auto points(info["points"].asUInt64());
    EXPECT_EQ(points, v.points());

    const Schema schema(info["schema"]);
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info["ticks"].asUInt64(), v.ticks());

    EXPECT_EQ(parse(a.get(outPath + "ept-sources/list.json")).size(), 8u);

    for (Json::ArrayIndex o(0); o < 8; ++o)
    {
        const auto path(metaPath + std::to_string(o) + ".json");
        const auto meta(parse(a.get(path)));
        ASSERT_FALSE(meta.isNull());
        ASSERT_GT(meta["points"].asUInt64(), 0u);
    }
}

TEST(build, reprojected)
{
    const std::string outPath(test::dataPath() + "out/ellipsoid-re/");
    const std::string metaPath(outPath + "ept-sources/");

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
    EXPECT_EQ(hierarchyType, "json");

    const auto points(info["points"].asUInt64());
    EXPECT_EQ(points, v.points());

    const Schema schema(info["schema"]);
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info["ticks"].asUInt64(), v.ticks());

    EXPECT_EQ(parse(a.get(outPath + "ept-sources/list.json")).size(), 8u);

    for (Json::ArrayIndex o(0); o < 8; ++o)
    {
        const auto path(metaPath + std::to_string(o) + ".json");
        const auto meta(parse(a.get(path)));
        ASSERT_FALSE(meta.isNull());
        ASSERT_GT(meta["points"].asUInt64(), 0u);
    }
}


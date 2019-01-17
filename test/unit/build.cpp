#include "gtest/gtest.h"
#include "config.hpp"
#include "verify.hpp"

#include <entwine/builder/builder.hpp>
#include <entwine/builder/merger.hpp>
#include <entwine/builder/scan.hpp>

using namespace entwine;

namespace
{
    const arbiter::Arbiter a;
    const Verify v;

    void checkSources(std::string outPath)
    {
        const json list(json::parse(a.get(outPath + "ept-sources/list.json")));
        EXPECT_EQ(list.size(), 8u);

        for (uint64_t o(0); o < 8; ++o)
        {
            const auto entry(list.at(o));
            const auto id(entry.at("id").get<std::string>());
            const auto url(entry.at("url").get<std::string>());
            const Bounds bounds(entry.at("bounds"));

            ASSERT_TRUE(bounds.exists());
            ASSERT_GT(id.size(), 0u);
            ASSERT_GT(url.size(), 0u);

            const json full(json::parse(a.get(outPath + "ept-sources/" + url)));
            ASSERT_FALSE(full.is_null());
            ASSERT_TRUE(full.count(id));

            const auto meta(full.at(id));
            ASSERT_FALSE(meta.is_null());
            ASSERT_GT(meta.at("points").get<uint64_t>(), 0u);
            ASSERT_EQ(meta.at("origin").get<Origin>(), (Origin)o);
            ASSERT_TRUE(meta.at("metadata").is_object());
        }
    }
}

TEST(build, basic)
{
    const std::string outPath(test::dataPath() + "out/ellipsoid/");
    const std::string metaPath(outPath + "ept-sources/");

    Config c(json {
        { "input", test::dataPath() + "ellipsoid-multi/" },
        { "output", outPath },
        { "force", true },
        { "span", v.span() },
        { "hierarchyStep", v.hierarchyStep() }
    });

    Builder(c).go();

    const json info(json::parse(a.get(outPath + "ept.json")));

    const Bounds bounds(info.at("bounds"));
    const Bounds boundsConforming(info.at("boundsConforming"));
    EXPECT_TRUE(bounds.isCubic());
    EXPECT_TRUE(bounds.contains(boundsConforming));
    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(boundsConforming[i], v.bounds()[i], 2.0) << "At: " << i <<
            "\n" << boundsConforming << "\n!=\n" << v.bounds() << std::endl;
    }

    const auto dataType(info.at("dataType").get<std::string>());
    EXPECT_EQ(dataType, "laszip");

    const auto hierarchyType(info.at("hierarchyType").get<std::string>());
    EXPECT_EQ(hierarchyType, "json");

    const auto points(info.at("points").get<uint64_t>());
    EXPECT_EQ(points, v.points());

    const Schema schema(info.at("schema"));
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info.at("span").get<uint64_t>(), v.span());

    checkSources(outPath);
}

TEST(build, continued)
{
    const std::string outPath(test::dataPath() + "out/ellipsoid/");
    const std::string metaPath(outPath + "ept-sources/");

    {
        Config c(json {
            { "input", test::dataPath() + "ellipsoid-multi/" },
            { "output", outPath },
            { "force", true },
            { "span", v.span() },
            { "hierarchyStep", v.hierarchyStep() },
            { "run", 4 }
        });

        Builder(c).go();
    }

    {
        Config c(json { { "output", outPath } });
        Builder(c).go();
    }

    const auto info(json::parse(a.get(outPath + "ept.json")));

    const Bounds bounds(info.at("bounds"));
    const Bounds boundsConforming(info.at("boundsConforming"));
    EXPECT_TRUE(bounds.isCubic());
    EXPECT_TRUE(bounds.contains(boundsConforming));
    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(boundsConforming[i], v.bounds()[i], 2.0) << "At: " << i <<
            "\n" << boundsConforming << "\n!=\n" << v.bounds() << std::endl;
    }

    const auto dataType(info.at("dataType").get<std::string>());
    EXPECT_EQ(dataType, "laszip");

    const auto hierarchyType(info.at("hierarchyType").get<std::string>());
    EXPECT_EQ(hierarchyType, "json");

    const auto points(info.at("points").get<uint64_t>());
    EXPECT_EQ(points, v.points());

    const Schema schema(info.at("schema"));
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info.at("span").get<uint64_t>(), v.span());

    checkSources(outPath);
}

TEST(build, addedLater)
{
    const std::string outPath(test::dataPath() + "out/ellipsoid/");
    const std::string metaPath(outPath + "ept-sources/");

    {
        Config c;
        c["input"].append(test::dataPath() + "ellipsoid-multi/ned.laz");
        c["input"].append(test::dataPath() + "ellipsoid-multi/neu.laz");
        c["input"].append(test::dataPath() + "ellipsoid-multi/nwd.laz");
        c["input"].append(test::dataPath() + "ellipsoid-multi/nwu.laz");
        c["output"] = outPath;
        c["force"] = true;
        c["span"] = static_cast<Json::UInt64>(v.span());
        c["hierarchyStep"] = static_cast<Json::UInt64>(v.hierarchyStep());
        c["bounds"] = v.bounds().toJson();

        Builder(c).go();
    }

    {
        Config c;
        c["input"].append(test::dataPath() + "ellipsoid-multi/sed.laz");
        c["input"].append(test::dataPath() + "ellipsoid-multi/seu.laz");
        c["input"].append(test::dataPath() + "ellipsoid-multi/swd.laz");
        c["input"].append(test::dataPath() + "ellipsoid-multi/swu.laz");
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

    EXPECT_EQ(info["span"].asUInt64(), v.span());

    checkSources(outPath);
}

TEST(build, fromScan)
{
    const std::string scanPath(test::dataPath() + "out/prebuild-scan/");

    {
        const std::string dataPath(test::dataPath() + "ellipsoid-multi");

        Config c(json {
            { "input", dataPath },
            { "output", scanPath }
        });
        Scan(c).go();
    }

    const std::string outPath(test::dataPath() + "out/from-scan/");
    const std::string metaPath(outPath + "ept-sources/");

    Config c(json {
        { "input", scanPath + "scan.json" },
        { "output", outPath },
        { "force", true },
        { "span", v.span() },
        { "hierarchyStep", v.hierarchyStep() }
    });

    Builder(c).go();

    const auto info(json::parse(a.get(outPath + "ept.json")));

    const Bounds bounds(info.at("bounds"));
    const Bounds boundsConforming(info.at("boundsConforming"));
    EXPECT_TRUE(bounds.isCubic());
    EXPECT_TRUE(bounds.contains(boundsConforming));
    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(boundsConforming[i], v.bounds()[i], 2.0) << "At: " << i <<
            "\n" << boundsConforming << "\n!=\n" << v.bounds() << std::endl;
    }

    const auto dataType(info.at("dataType").get<std::string>());
    EXPECT_EQ(dataType, "laszip");

    const auto hierarchyType(info.at("hierarchyType").get<std::string>());
    EXPECT_EQ(hierarchyType, "json");

    const auto points(info.at("points").get<uint64_t>());
    EXPECT_EQ(points, v.points());

    const Schema schema(info.at("schema"));
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info.at("span").get<uint64_t>(), v.span());

    checkSources(outPath);
}

TEST(build, subset)
{
    const std::string outPath(test::dataPath() + "out/subset/");
    const std::string metaPath(outPath + "ept-sources/");

    for (Json::UInt64 i(0); i < 4u; ++i)
    {
        Config c(json {
            { "input", test::dataPath() + "ellipsoid-multi/" },
            { "output", outPath },
            { "force", true },
            { "span", v.span() },
            { "hierarchyStep", v.hierarchyStep() },
            { "subset", {
                { "id", i + 1 },
                { "of", 4 }
            } }
        });

        Builder(c).go();
    }

    {
        Config c(json { { "output", outPath } });
        Merger(c).go();
    }

    const auto info(json::parse(a.get(outPath + "ept.json")));

    const Bounds bounds(info.at("bounds"));
    const Bounds boundsConforming(info.at("boundsConforming"));
    EXPECT_TRUE(bounds.isCubic());
    EXPECT_TRUE(bounds.contains(boundsConforming));
    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(boundsConforming[i], v.bounds()[i], 2.0) << "At: " << i <<
            "\n" << boundsConforming << "\n!=\n" << v.bounds() << std::endl;
    }

    const auto dataType(info.at("dataType").get<std::string>());
    EXPECT_EQ(dataType, "laszip");

    const auto hierarchyType(info.at("hierarchyType").get<std::string>());
    EXPECT_EQ(hierarchyType, "json");

    const auto points(info.at("points").get<uint64_t>());
    EXPECT_EQ(points, v.points());

    const Schema schema(info.at("schema"));
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info.at("span").get<uint64_t>(), v.span());

    checkSources(outPath);
}

TEST(build, invalidSubset)
{
    const std::string outPath(test::dataPath() + "out/subset/");

    Config c(json {
        { "input", test::dataPath() + "ellipsoid-multi/" },
        { "output", outPath },
        { "force", true },
        { "span", v.span() },
        { "hierarchyStep", v.hierarchyStep() },
        { "subset", {
            { "id", 1 }
        } }
    });

    // Invalid subset range - must be more than one subset.
    c.setSubsetOf(1);
    EXPECT_ANY_THROW(Builder(c).go());

    // Invalid subset range - must be a perfect square.
    c.setSubsetOf(8);
    EXPECT_ANY_THROW(Builder(c).go());

    // Invalid subset range - must be a power of 2.
    c.setSubsetOf(9);
    EXPECT_ANY_THROW(Builder(c).go());

    // Invalid subset range.
    c.setSubsetOf(3320);
    EXPECT_ANY_THROW(Builder(c).go());

    c.setSubsetOf(4);

    // Invalid subset ID - must be 1-based.
    c.setSubsetId(0);
    EXPECT_ANY_THROW(Builder(c).go());

    // Invalid subset ID - must be less than or equal to total subsets.
    c.setSubsetId(5);
    EXPECT_ANY_THROW(Builder(c).go());
}

TEST(build, subsetFromScan)
{
    const std::string scanPath(test::dataPath() + "out/prebuild-scan/");

    {
        const std::string dataPath(test::dataPath() + "ellipsoid-multi");

        json c {
            { "input", dataPath },
            { "output", scanPath }
        };
        Scan(c).go();
    }

    const std::string outPath(test::dataPath() + "out/from-scan-subset/");
    const std::string metaPath(outPath + "ept-sources/");

    for (Json::UInt64 i(0); i < 4u; ++i)
    {
        Config c(json {
            { "input", scanPath + "scan.json" },
            { "output", outPath },
            { "force", true },
            { "span", v.span() },
            { "hierarchyStep", v.hierarchyStep() },
            { "subset", {
                { "id", i + 1 },
                { "of", 4 }
            } }
        });

        Builder(c).go();
    }

    {
        Config c(json { { "output", outPath } });
        Merger(c).go();
    }

    const auto info(json::parse(a.get(outPath + "ept.json")));

    const Bounds bounds(info.at("bounds"));
    const Bounds boundsConforming(info.at("boundsConforming"));
    EXPECT_TRUE(bounds.isCubic());
    EXPECT_TRUE(bounds.contains(boundsConforming));
    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(boundsConforming[i], v.bounds()[i], 2.0) << "At: " << i <<
            "\n" << boundsConforming << "\n!=\n" << v.bounds() << std::endl;
    }

    const auto dataType(info.at("dataType").get<std::string>());
    EXPECT_EQ(dataType, "laszip");

    const auto hierarchyType(info.at("hierarchyType").get<std::string>());
    EXPECT_EQ(hierarchyType, "json");

    const auto points(info.at("points").get<uint64_t>());
    EXPECT_EQ(points, v.points());

    const Schema schema(info.at("schema"));
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info.at("span").get<uint64_t>(), v.span());

    checkSources(outPath);
}

TEST(build, reprojected)
{
    if (arbiter::util::env("APPVEYOR"))
    {
        std::cout << "Skipping reprojection tests" << std::endl;
        return;
    }

    const std::string outPath(test::dataPath() + "out/ellipsoid-re/");
    const std::string metaPath(outPath + "ept-sources/");

    Config c(json {
        { "input", test::dataPath() + "ellipsoid-multi/" },
        { "output", outPath },
        { "reprojection", {
            { "out", "EPSG:26918" }
        } },
        { "force", true },
        { "span", v.span() },
        { "hierarchyStep", v.hierarchyStep() }
    });

    Builder(c).go();

    const auto info(json::parse(a.get(outPath + "ept.json")));

    const Bounds bounds(info.at("bounds"));
    const Bounds boundsConforming(info.at("boundsConforming"));
    EXPECT_TRUE(bounds.isCubic());
    EXPECT_TRUE(bounds.contains(boundsConforming));
    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(boundsConforming[i], v.boundsUtm()[i], 2.0) <<
            "At: " << i <<
            "\n" << boundsConforming << "\n!=\n" << v.boundsUtm() << std::endl;
    }

    const auto dataType(info.at("dataType").get<std::string>());
    EXPECT_EQ(dataType, "laszip");

    const auto hierarchyType(info.at("hierarchyType").get<std::string>());
    EXPECT_EQ(hierarchyType, "json");

    const auto points(info.at("points").get<uint64_t>());
    EXPECT_EQ(points, v.points());

    const Schema schema(info.at("schema"));
    Schema verifySchema(v.schema().append(DimId::OriginId));
    verifySchema.setOffset(bounds.mid().round());
    EXPECT_EQ(schema, verifySchema);

    EXPECT_EQ(info.at("span").get<uint64_t>(), v.span());

    checkSources(outPath);
}


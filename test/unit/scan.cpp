#include "gtest/gtest.h"
#include "config.hpp"
#include "verify.hpp"

#include <entwine/builder/scan.hpp>
#include <entwine/util/executor.hpp>

using namespace entwine;
using DimId = pdal::Dimension::Id;
using DimType = pdal::Dimension::Type;

namespace
{
    const Verify v;
}

TEST(scan, empty)
{
    Json::Value in;
    in["input"] = test::dataPath() + "not-an-existing-directory";

    Scan scan(in);
    const Json::Value out(scan.go().json());
    EXPECT_EQ(out, Json::nullValue);
}

TEST(scan, single)
{
    Json::Value in;
    in["input"] = test::dataPath() + "ellipsoid.laz";
    const Config out(Scan(in).go());
    ASSERT_FALSE(out.json().isNull());

    const Bounds bounds(out["bounds"]);
    const auto delta(out.delta());
    const Schema schema(out.schema());

    EXPECT_EQ(bounds, v.bounds());
    ASSERT_TRUE(delta);
    EXPECT_EQ(delta->scale(), v.scale());
    EXPECT_EQ(out.numPoints(), v.numPoints());
    ASSERT_EQ(schema, v.schema());

    const FileInfoList input(out.input());
    ASSERT_EQ(input.size(), 1u);

    const FileInfo file(input.at(0));
    const auto expFile(Executor::get().preview(file.path()));
    ASSERT_TRUE(expFile);

    EXPECT_EQ(arbiter::util::getBasename(file.path()), "ellipsoid.laz");
    ASSERT_TRUE(file.bounds());
    EXPECT_EQ(*file.bounds(), v.bounds());
    EXPECT_EQ(file.numPoints(), v.numPoints());
    EXPECT_EQ(file.srs().getWKT(), expFile->srs);

    EXPECT_EQ(out.srs(), expFile->srs);
}

TEST(scan, multi)
{
    Json::Value in;
    in["input"] = test::dataPath() + "ellipsoid-multi";

    const Config out(Scan(in).go());
    ASSERT_FALSE(out.json().isNull());

    const Bounds bounds(out["bounds"]);
    const auto delta(out.delta());
    const Schema schema(out.schema());

    EXPECT_EQ(bounds, v.bounds());
    ASSERT_TRUE(delta);
    EXPECT_EQ(delta->scale(), v.scale());
    EXPECT_EQ(out.numPoints(), v.numPoints());
    ASSERT_EQ(schema, v.schema());

    const FileInfoList input(out.input());
    ASSERT_EQ(input.size(), 8u);

    std::map<std::string, bool> basenames {
        { "ned.laz", false },
        { "neu.laz", false },
        { "nwd.laz", false },
        { "nwu.laz", false },
        { "sed.laz", false },
        { "seu.laz", false },
        { "swd.laz", false },
        { "swu.laz", false }
    };

    for (const auto file : input)
    {
        const auto path(file.path());
        const auto basename(arbiter::util::getBasename(path));
        const auto expFile(Executor::get().preview(file.path()));

        ASSERT_TRUE(expFile) << path;
        ASSERT_TRUE(basenames.count(basename)) << path;
        ASSERT_FALSE(basenames.at(basename)) << path;
        basenames.at(basename) = true;

        ASSERT_TRUE(file.bounds()) << path;
        ASSERT_EQ(*file.bounds(), expFile->bounds) << path;
        ASSERT_EQ(file.numPoints(), expFile->numPoints) << path;
        ASSERT_EQ(file.srs().getWKT(), expFile->srs) << path;
        ASSERT_EQ(out.srs(), expFile->srs) << path;
    }
}

TEST(scan, reprojection)
{
    Json::Value in;
    in["input"] = test::dataPath() + "ellipsoid.laz";
    in["reprojection"]["out"] = "EPSG:26918";

    const Config out(Scan(in).go());
    ASSERT_FALSE(out.json().isNull());

    const Bounds bounds(out["bounds"]);
    const auto delta(out.delta());
    const Schema schema(out.schema());

    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(bounds[i], v.boundsUtm()[i], 1.0);
    }

    ASSERT_TRUE(delta);
    EXPECT_EQ(delta->scale(), v.scale());
    EXPECT_EQ(out.numPoints(), v.numPoints());
    ASSERT_EQ(schema, v.schema());

    const FileInfoList input(out.input());
    ASSERT_EQ(input.size(), 1u);

    const FileInfo file(input.at(0));
    const auto expFile(Executor::get().preview(file.path()));
    ASSERT_TRUE(expFile);

    EXPECT_EQ(arbiter::util::getBasename(file.path()), "ellipsoid.laz");
    ASSERT_TRUE(file.bounds());
    EXPECT_EQ(*file.bounds(), bounds);
    EXPECT_EQ(file.numPoints(), v.numPoints());
    EXPECT_NE(file.srs().getWKT(), expFile->srs);

    EXPECT_EQ(out.srs(), in["reprojection"]["out"].asString());
}

TEST(scan, reprojectionHammer)
{
    Json::Value in;
    in["input"] = test::dataPath() + "ellipsoid-wrong-srs.laz";
    in["reprojection"]["in"] = "EPSG:3857";
    in["reprojection"]["out"] = "EPSG:26918";
    in["reprojection"]["hammer"] = true;

    const Config out(Scan(in).go());
    ASSERT_FALSE(out.json().isNull());

    const Bounds bounds(out["bounds"]);
    const auto delta(out.delta());
    const Schema schema(out.schema());

    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(bounds[i], v.boundsUtm()[i], 1.0);
    }

    ASSERT_TRUE(delta);
    EXPECT_EQ(delta->scale(), v.scale());
    EXPECT_EQ(out.numPoints(), v.numPoints());
    ASSERT_EQ(schema, v.schema());

    const FileInfoList input(out.input());
    ASSERT_EQ(input.size(), 1u);

    const FileInfo file(input.at(0));
    const auto expFile(Executor::get().preview(file.path()));
    ASSERT_TRUE(expFile);

    EXPECT_EQ(arbiter::util::getBasename(
                file.path()), "ellipsoid-wrong-srs.laz");
    ASSERT_TRUE(file.bounds());
    EXPECT_EQ(*file.bounds(), bounds);
    EXPECT_EQ(file.numPoints(), v.numPoints());
    EXPECT_NE(file.srs().getWKT(), expFile->srs);

    EXPECT_EQ(out.srs(), in["reprojection"]["out"].asString());
}

TEST(scan, outputFile)
{
    Json::Value in;
    in["input"] = test::dataPath() + "ellipsoid.laz";
    in["output"] = test::dataPath() + "out/scan.json";
    Scan(in).go();
    const Config out(parse(arbiter::Arbiter().get(in["output"].asString())));
    ASSERT_FALSE(out.json().isNull());

    const Bounds bounds(out["bounds"]);
    const auto delta(out.delta());
    const Schema schema(out.schema());

    EXPECT_EQ(bounds, v.bounds());
    ASSERT_TRUE(delta);
    EXPECT_EQ(delta->scale(), v.scale());
    EXPECT_EQ(out.numPoints(), v.numPoints());
    ASSERT_EQ(schema, v.schema());

    const FileInfoList input(out.input());
    ASSERT_EQ(input.size(), 1u);

    const FileInfo file(input.at(0));
    const auto expFile(Executor::get().preview(file.path()));
    ASSERT_TRUE(expFile);

    EXPECT_EQ(arbiter::util::getBasename(file.path()), "ellipsoid.laz");
    ASSERT_TRUE(file.bounds());
    EXPECT_EQ(*file.bounds(), v.bounds());
    EXPECT_EQ(file.numPoints(), v.numPoints());
    EXPECT_EQ(file.srs().getWKT(), expFile->srs);

    EXPECT_EQ(out.srs(), expFile->srs);
}


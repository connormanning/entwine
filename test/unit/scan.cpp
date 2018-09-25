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

TEST(scan, nonexistentDirectory)
{
    Json::Value in;
    in["input"] = test::dataPath() + "not-an-existing-directory";
    Scan scan(in);
    EXPECT_ANY_THROW(scan.go());
}

TEST(scan, nonexistentFile)
{
    Json::Value in;
    in["input"] = test::dataPath() + "not-an-existing-file.laz";
    Scan scan(in);
    EXPECT_ANY_THROW(scan.go());
}

TEST(scan, single)
{
    Json::Value in;
    in["input"] = test::dataPath() + "ellipsoid.laz";
    const Config out(Scan(in).go());
    ASSERT_FALSE(out.json().isNull());

    const Bounds bounds(out["bounds"]);
    const Schema schema(out.schema());

    EXPECT_EQ(bounds, v.bounds());
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

    EXPECT_EQ(out.srs().wkt(), expFile->srs);
}

TEST(scan, multi)
{
    Json::Value in;
    in["input"] = test::dataPath() + "ellipsoid-multi";

    const Config out(Scan(in).go());
    ASSERT_FALSE(out.json().isNull());

    const Bounds bounds(out["bounds"]);
    const Schema schema(out.schema());

    EXPECT_EQ(bounds, v.bounds());
    EXPECT_EQ(out.numPoints(), v.numPoints());
    ASSERT_EQ(schema, v.schema());

    const FileInfoList input(out.input());
    ASSERT_EQ(input.size(), 9u);

    std::map<std::string, bool> basenames {
        { "ned.laz", false },
        { "neu.laz", false },
        { "nwd.laz", false },
        { "nwu.laz", false },
        { "sed.laz", false },
        { "seu.laz", false },
        { "swd.laz", false },
        { "swu.laz", false },
        { "zzz.txt", false }
    };

    for (uint64_t i(0); i < 8u; ++i)
    {
        const auto& file(input.at(i));
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
        ASSERT_EQ(out.srs().wkt(), expFile->srs) << path;
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
    const Schema schema(out.schema());

    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(bounds[i], v.boundsUtm()[i], 1.0);
    }

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

    EXPECT_EQ(out.srs().codeString(), in["reprojection"]["out"].asString());
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
    const Schema schema(out.schema());

    for (std::size_t i(0); i < 6; ++i)
    {
        ASSERT_NEAR(bounds[i], v.boundsUtm()[i], 1.0);
    }

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

    EXPECT_EQ(out.srs().codeString(), in["reprojection"]["out"].asString());
}

TEST(scan, outputFile)
{
    Json::Value in;
    in["input"] = test::dataPath() + "ellipsoid.laz";
    in["output"] = test::dataPath() + "out/scan/";
    Scan(in).go();
    const std::string path(test::dataPath() + "out/scan/ept-scan.json");
    const Config out(parse(arbiter::Arbiter().get(path)));
    ASSERT_FALSE(out.json().isNull());

    const Bounds bounds(out["bounds"]);
    const Schema schema(out.schema());

    EXPECT_EQ(bounds, v.bounds());
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

    EXPECT_EQ(out.srs().wkt(), expFile->srs);
}


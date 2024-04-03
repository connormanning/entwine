#include "gtest/gtest.h"
#include "config.hpp"

#include <pdal/PipelineManager.hpp>

#include <entwine/builder/builder.hpp>
#include <entwine/io/laszip.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/config.hpp>
#include <entwine/util/json.hpp>

using namespace entwine;

namespace
{
    struct Stuff
    {
        pdal::PipelineManager pm;
        pdal::PointViewPtr view;
    };

    arbiter::Arbiter a;
    const std::string outDir(test::dataPath() + "out/ellipsoid/");
    const std::string outFile(outDir + "ept.json");

    const Bounds bounds(-8242747, 4966455, -151, -8242445, 4966757, 151);
    const Bounds boundsConforming(-8242747, 4966505, -51, -8242445, 4966707, 51);

    const Bounds boundsUtm(580620, 4504579, -116, 580852, 4504811, 116);
    const Bounds boundsConformingUtm(
        580621, 4504618, -51, 580851, 4504772, 51);

    const uint64_t points = 100000;
    const Srs srs("EPSG:3857");
    const StringList dimensions = {
        "X", "Y", "Z", "Intensity", "ReturnNumber", "NumberOfReturns",
        "ScanDirectionFlag", "EdgeOfFlightLine", "Classification",
        "ScanAngleRank", "UserData", "PointSourceId", "GpsTime", "Red", "Green",
        "Blue"
    };

    uint64_t run(const json partial)
    {
        const json defaults = {
            { "output", outDir },
            { "force", true },
            { "span", 32 },
            { "hierarchyStep", 2 },
            { "progressInterval", 0 },
            { "verbose", false }
        };
        const json j = merge(defaults, partial);

        Builder builder = builder::create(j);
        return builder::run(builder, j);
    }

    json checkEpt()
    {
        const json ept = json::parse(a.get(outFile));
        EXPECT_EQ(Bounds(ept.at("bounds")), bounds);
        EXPECT_EQ(Bounds(ept.at("boundsConforming")), boundsConforming);
        EXPECT_EQ(ept.at("hierarchyType").get<std::string>(), "json");
        EXPECT_EQ(ept.at("points").get<int>(), 100000);
        EXPECT_EQ(ept.at("span").get<int>(), 32);
        EXPECT_EQ(ept.at("srs").get<Srs>(), srs);
        EXPECT_EQ(ept.at("version").get<std::string>(), "1.1.0");
        const Schema schema = ept.at("schema").get<Schema>();
        EXPECT_TRUE(hasStats(schema));
        for (const auto& name : dimensions) EXPECT_TRUE(contains(schema, name));
        return ept;
    }

    std::unique_ptr<Stuff> execute(const json pipeline = { outFile })
    {
        auto stuff = makeUnique<Stuff>();
        auto& pm = stuff->pm;

        std::istringstream iss(pipeline.dump());
        pm.readPipeline(iss);
        pm.prepare();
        pm.execute();

        const auto set = pm.views();
        if (!set.empty())
        {
            stuff->view = (*set.begin())->makeNew();
            for (const auto& current : set) stuff->view->append(*current);
        }
        return stuff;
    }

    void checkData(pdal::PointView& view, const Bounds b = boundsConforming)
    {
        for (const auto pr : view)
        {
            const Point point(
                pr.getFieldAs<double>(pdal::Dimension::Id::X),
                pr.getFieldAs<double>(pdal::Dimension::Id::Y),
                pr.getFieldAs<double>(pdal::Dimension::Id::Z));
            ASSERT_TRUE(b.contains(point));
        }
    }
}

TEST(build, laszip)
{
    run({ { "input", test::dataPath() + "ellipsoid.laz" } });
    const json ept = checkEpt();
    // By default, since this is laszip input, our output will be laszip.
    EXPECT_EQ(ept.at("dataType").get<std::string>(), "laszip");

    const auto stuff = execute();
    auto& view = stuff->view;
    ASSERT_TRUE(view);
    checkData(*view);
}

TEST(build, laszip14)
{
    // This file has for every point:
    //   Classification=33
    //   KeyPoint=1
    run({ 
        { "input", test::dataPath() + "ellipsoid14.laz" },
        { "laz_14", true }
    });
    const json ept = checkEpt();
    EXPECT_EQ(ept.at("dataType").get<std::string>(), "laszip");

    const auto stuff = execute();
    auto& view = stuff->view;
    ASSERT_TRUE(view);
    checkData(*view);
    for (const auto pr : *view)
    {
        ASSERT_EQ(pr.getFieldAs<int>(pdal::Dimension::Id::Classification), 33);
        ASSERT_TRUE(pr.getFieldAs<bool>(pdal::Dimension::Id::KeyPoint));
    }
}

TEST(build, failedWrite)
{
    struct FailIo : public io::Laszip
    {
        FailIo(const Metadata& metadata, const Endpoints& endpoints)
            : Laszip(metadata, endpoints)
        { }

        virtual void write(
            const std::string filename,
            BlockPointTable& table,
            const Bounds bounds) const override
        {
            throw std::runtime_error("Faking IO failure");
        }
    };

    const json config = {
        { "input", test::dataPath() + "ellipsoid.laz" },
        { "output", outDir },
        { "force", true },
        { "verbose", false }
    };
    Builder builder = builder::create(config);
    builder.io.reset(new FailIo(builder.metadata, builder.endpoints));
    ASSERT_ANY_THROW(builder::run(builder, config));
}

TEST(build, binary)
{
    run({
        { "input", test::dataPath() + "ellipsoid.laz" },
        { "dataType", "binary" }
    });
    const json ept = checkEpt();
    EXPECT_EQ(ept.at("dataType").get<std::string>(), "binary");

    const auto stuff = execute();
    auto& view = stuff->view;
    ASSERT_TRUE(view);
    checkData(*view);
}

#ifndef NO_ZSTD
TEST(build, zstandard)
{
    run({
        { "input", test::dataPath() + "ellipsoid.laz" },
        { "dataType", "zstandard" }
    });
    const json ept = checkEpt();
    EXPECT_EQ(ept.at("dataType").get<std::string>(), "zstandard");

    const auto stuff = execute();
    auto& view = stuff->view;
    ASSERT_TRUE(view);
    checkData(*view);
}
#endif

/*
TEST(build, directory)
{
    run({ { "input", test::dataPath() + "ellipsoid-multi" } });
    const json ept = checkEpt();

    const json pipeline = {
        { { "filename", outFile }, { "origin", "ned" } }
    };
    const auto stuff = execute(pipeline);
    auto& view = stuff->view;
    ASSERT_TRUE(view);
    ASSERT_GT(view->size(), 0);

    // The selected "ned" file contains only the points in the north-east-down
    // octant of the dataset, so make sure they fit in those smaller bounds.
    const auto ned = boundsConforming.getNed();
    for (const auto pr : *view)
    {
        const Point point(
            pr.getFieldAs<double>(pdal::Dimension::Id::X),
            pr.getFieldAs<double>(pdal::Dimension::Id::Y),
            pr.getFieldAs<double>(pdal::Dimension::Id::Z));
        EXPECT_TRUE(ned.contains(point));
    }
}
*/

TEST(build, reprojected)
{
    run({
        { "input", test::dataPath() + "ellipsoid.laz" },
        { "reprojection", { { "out", "EPSG:26918" } } }
    });
    const json ept = json::parse(a.get(outFile));
    EXPECT_EQ(Bounds(ept.at("bounds")), boundsUtm);
    EXPECT_EQ(Bounds(ept.at("boundsConforming")), boundsConformingUtm);
    EXPECT_EQ(Srs(ept.at("srs")), Srs("EPSG:26918"));

    const auto stuff = execute();
    auto& view = stuff->view;
    ASSERT_TRUE(view);
    checkData(*view, boundsConformingUtm);
}


TEST(build, continued)
{
    const std::string input = test::dataPath() + "ellipsoid-multi";
    run({ { "input", input }, { "limit", 4 } });

    // After the partial run, we should have partial data.
    {
        const auto stuff = execute();
        auto& view = stuff->view;
        ASSERT_TRUE(view);
        ASSERT_TRUE(view->size() < points);
    }

    // Now run the rest.
    run({ { "input", input }, { "force", false } });

    const json ept = checkEpt();

    const auto stuff = execute();
    auto& view = stuff->view;
    ASSERT_TRUE(view);
    checkData(*view);
}

TEST(build, subset)
{
    const std::string input = test::dataPath() + "ellipsoid-multi";
    for (int i = 0; i < 4; ++i)
    {
        const json config = {
            { "input", input },
            { "subset", {
                { "id", i + 1 },
                { "of", 4 }
            } }
        };
        run(config);
    }

    builder::merge({
        { "output", outDir },
        { "force", true },
        { "verbose", false },
    });

    const json ept = checkEpt();

    const auto stuff = execute();
    auto& view = stuff->view;
    ASSERT_TRUE(view);
    checkData(*view);
}

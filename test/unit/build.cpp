#include "gtest/gtest.h"
#include "config.hpp"

#include <pdal/Dimension.hpp>
#include <pdal/util/FileUtils.hpp>
#include <pdal/util/Utils.hpp>

#include "entwine/reader/cache.hpp"
#include "entwine/reader/reader.hpp"
#include "entwine/third/arbiter/arbiter.hpp"
#include "entwine/tree/builder.hpp"
#include "entwine/tree/config-parser.hpp"
#include "entwine/tree/inference.hpp"
#include "entwine/tree/merger.hpp"
#include "entwine/types/vector-point-table.hpp"
#include "entwine/util/json.hpp"

#include "octree.hpp"

using namespace entwine;

namespace
{
    const std::string outPath(test::dataPath() + "out");
    const std::string binPath(test::binaryPath() + "entwine ");

    int run(std::string command)
    {
        std::string output;
        return pdal::Utils::run_shell_command(binPath + command, output);
    }

    const Bounds actualBounds(-150, -100, -50, 150, 100, 50);

    using D = pdal::Dimension::Id;
    const std::vector<pdal::Dimension::Id> actualDims
    {
        D::X, D::Y, D::Z,
        D::Intensity,
        D::ReturnNumber, D::NumberOfReturns,
        D::EdgeOfFlightLine, D::Classification,
        D::ScanAngleRank,
        D::PointSourceId, D::GpsTime,
        D::Red, D::Green, D::Blue,
        D::OriginId
    };
}

struct Expectations
{
    Expectations(
            const Json::Value& config,
            Bounds boundsConforming,
            Delta delta = Delta())
        : config(config)
        , boundsConforming(boundsConforming)
        , delta(delta)
    { }

    Json::Value config;
    Bounds boundsConforming;
    Delta delta;
};

std::ostream& operator<<(std::ostream& os, const Expectations& e)
{
    os <<
        " B: " << e.boundsConforming;

    return os;
}

struct BuildTest :
    public testing::Test,
    public testing::WithParamInterface<Expectations>
{
protected:
    BuildTest() : expect(GetParam()) { }

    virtual void SetUp() override { cleanup(); }
    virtual void TearDown() override { cleanup(); }
    void cleanup()
    {
        for (const auto p : arbiter::Arbiter().resolve(outPath + "/**"))
        {
            pdal::FileUtils::deleteFile(p);
        }
    }

    const Expectations expect;
};

TEST_P(BuildTest, Verify)
{
    auto build([](const Json::Value& config, bool isContinuation)
    {
        auto builder(ConfigParser::getBuilder(config));
        ASSERT_TRUE(builder);
        ASSERT_EQ(builder->isContinuation(), isContinuation);
        builder->go(config["run"].asUInt64());
    });

    arbiter::Arbiter a;
    arbiter::Endpoint outEp(a.getEndpoint(outPath));

    Json::Value config(expect.config);

    if (config.isMember("run"))
    {
        const std::size_t run(config["run"].asUInt64());
        std::size_t ran(0);
        std::size_t total(0);

        do
        {
            build(config, ran);
            const Json::Value mani(parse(outEp.get("entwine-manifest")));
            total = mani["fileInfo"].size();
            ran += run;
        }
        while (run && ran < total);
    }
    else if (config.isMember("subset"))
    {
        std::size_t id(0);
        const std::size_t of(config["subset"]["of"].asUInt64());

        while (id < of)
        {
            // Subset IDs are one-based.
            Json::Value current(config);
            current["subset"]["id"] = Json::UInt64(id + 1);
            ++id;
            build(current, false);
        }

        entwine::Merger merger(config["output"].asString(), 1);
        merger.go();
    }
    else
    {
        build(config, false);
    }

    const Json::Value meta(parse(outEp.get("entwine")));

    const Metadata metadata(meta);
    const Manifest manifest(parse(outEp.get("entwine-manifest")), outEp);
    const Delta delta(Delta::existsIn(meta) ? Delta(meta) : Delta());

    // Delta.
    if (expect.delta.empty())
    {
        EXPECT_FALSE(Delta::existsIn(meta));
    }
    else
    {
        EXPECT_EQ(delta.scale(), expect.delta.scale());
        EXPECT_EQ(delta.offset(), expect.delta.offset());
    }

    // Bounds.
    const Bounds bounds(meta["bounds"]);
    const Bounds boundsConforming(meta["boundsConforming"]);

    EXPECT_EQ(boundsConforming, expect.boundsConforming);

    EXPECT_TRUE(bounds.isCubic());
    EXPECT_TRUE(bounds.contains(boundsConforming));

    // Schema.
    const Schema schema(meta["schema"]);
    for (const auto d : actualDims)
    {
        EXPECT_TRUE(schema.contains(pdal::Dimension::name(d)));
    }

    // Miscellaneous parameters.
    EXPECT_EQ(
            meta["storage"].asString(),
            config["absolute"].asBool() ? "lazperf" : "laszip");

    EXPECT_EQ(meta["compressHierarchy"].asString(), "lzma");

    EXPECT_EQ(
            meta["trustHeaders"].asBool(),
            config.isMember("trustHeaders") ?
                config["trustHeaders"].asBool() : true);

    EXPECT_EQ(Version(meta["version"].asString()), currentVersion());

    // Verify results against a simple octree implementation.
    Cache cache(32);
    Reader r(outPath, cache);

    const Delta empty;
    test::Octree o(
            bounds,
            empty,
            meta["structure"]["nullDepth"].asUInt64(),
            meta["structure"]["coldDepth"].asUInt64());

    for (std::size_t i(0); i < manifest.size(); ++i)
    {
        o.insert(manifest.get(i).path());
    }

    EXPECT_EQ(o.inserts(), manifest.pointStats().inserts());
    EXPECT_EQ(o.outOfBounds(), manifest.pointStats().outOfBounds());

    using DimId = pdal::Dimension::Id;

    const std::map<Dir, Origin> alphabetical {
        { Dir::ned, 0 },
        { Dir::neu, 1 },
        { Dir::nwd, 2 },
        { Dir::nwu, 3 },
        { Dir::sed, 4 },
        { Dir::seu, 5 },
        { Dir::swd, 6 },
        { Dir::swu, 7 }
    };

    // Check that each depth has the same point count.
    std::size_t depth(0);
    bool pointsFound(false), pointsEnded(false);
    while (!pointsEnded)
    {
        const auto data(r.query(depth));
        const std::size_t np(data.size() / schema.pointSize());
        ASSERT_EQ(np, o.query(depth).size());

        VectorPointTable table(schema, data);
        pdal::PointRef pr(table, 0);

        if (!config["single"].asBool())
        {
            for (std::size_t i(0); i < np; ++i)
            {
                pr.setPointId(i);
                const Point p(
                        pr.getFieldAs<double>(DimId::X),
                        pr.getFieldAs<double>(DimId::Y),
                        pr.getFieldAs<double>(DimId::Z));

                if (p.x && p.y && p.z)
                {
                    const auto origin(pr.getFieldAs<Origin>(DimId::OriginId));
                    const Dir dir(getDirection(Point(), p));
                    ASSERT_EQ(alphabetical.at(dir), origin);
                }
            }
        }

        if (np) pointsFound = true;
        else if (pointsFound) pointsEnded = true;

        Bounds q(bounds);
        for (std::size_t n(0); n < 3; ++n)
        {
            q = q.get(toDir((depth + n) % 8));
            const std::size_t np(r.query(q, depth).size() / schema.pointSize());

            ASSERT_EQ(np, o.query(q, depth).size()) <<
                " B: " << bounds << " D: " << depth << std::endl;

        }

        if (depth >= meta["hierarchyStructure"]["startDepth"].asUInt64())
        {
            const auto j = r.hierarchy(bounds, depth, depth + 1);
            const std::size_t h(j.isNull() ? 0 : j["n"].asUInt64());
            ASSERT_EQ(np, h);
        }

        ++depth;
    }
}

namespace absolute
{
    Json::Value single(([]()
    {
        Json::Value json;
        json["input"] = test::dataPath() + "ellipsoid-single-laz";
        json["output"] = outPath;
        json["absolute"] = true;
        json["single"] = true;
        return json;
    })());

    Json::Value multi(([]()
    {
        Json::Value json;
        json["input"] = test::dataPath() + "ellipsoid-multi-laz";
        json["output"] = outPath;
        json["absolute"] = true;
        return json;
    })());

    Json::Value continued(([]()
    {
        Json::Value json;
        json["input"] = test::dataPath() + "ellipsoid-multi-laz";
        json["output"] = outPath;
        json["absolute"] = true;
        json["run"] = 4;
        return json;
    })());

    Json::Value subset(([]()
    {
        Json::Value json;
        json["input"] = test::dataPath() + "ellipsoid-multi-laz";
        json["output"] = outPath;
        json["absolute"] = true;
        json["subset"]["of"] = 16;
        return json;
    })());

    Expectations one(single, actualBounds);
    Expectations two(multi, actualBounds);
    Expectations con(continued, actualBounds);
    Expectations sub(subset, actualBounds);

    INSTANTIATE_TEST_CASE_P(
            Absolute,
            BuildTest,
            testing::Values(one, two, con, sub), );
}

namespace scaled
{
    Json::Value single(([]()
    {
        Json::Value json;
        json["input"] = test::dataPath() + "ellipsoid-single-laz";
        json["output"] = outPath;
        json["single"] = true;
        return json;
    })());

    Json::Value multi(([]()
    {
        Json::Value json;
        json["input"] = test::dataPath() + "ellipsoid-multi-laz";
        json["output"] = outPath;
        return json;
    })());

    Json::Value continued(([]()
    {
        Json::Value json;
        json["input"] = test::dataPath() + "ellipsoid-multi-laz";
        json["output"] = outPath;
        json["run"] = 4;
        return json;
    })());

    Json::Value subset(([]()
    {
        Json::Value json;
        json["input"] = test::dataPath() + "ellipsoid-multi-laz";
        json["output"] = outPath;
        json["subset"]["of"] = 16;
        return json;
    })());

    const Delta delta(Scale(.01));

    Expectations one(single, actualBounds, delta);
    Expectations two(multi, actualBounds, delta);
    Expectations con(continued, actualBounds, delta);
    Expectations sub(subset, actualBounds, delta);

    INSTANTIATE_TEST_CASE_P(
            Scaled,
            BuildTest,
            testing::Values(one, two, con, sub), );
}

TEST(Build, Kernel)
{
    std::string output;
    int status(0);

    status = run("build");
    EXPECT_EQ(status, 0);

    // TODO Test other CLI invocations.
}


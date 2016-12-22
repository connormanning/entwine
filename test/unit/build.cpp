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
#include "entwine/util/inference.hpp"
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

    arbiter::Arbiter a;
    arbiter::Endpoint outEp(a.getEndpoint(outPath));

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
        D::PointId, D::OriginId
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
        for (const auto p : a.resolve(outPath + "/**"))
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

    Json::Value config(expect.config);
    if (config.isArray()) config = config[0];

    if (expect.config.isObject())
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
    else
    {
        for (Json::ArrayIndex i(0); i < expect.config.size(); ++i)
        {
            build(expect.config[i], i != 0);
        }
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
            meta["compress"].asBool(),
            config.isMember("compress") ? config["compress"].asBool() : true);

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

    // Check that each depth has the same point count.
    std::size_t depth(0);
    bool pointsFound(false), pointsEnded(false);
    while (!pointsEnded)
    {
        const std::size_t np(r.query(depth).size() / schema.pointSize());
        ASSERT_EQ(np, o.query(depth).size());

        if (np) pointsFound = true;
        else if (pointsFound) pointsEnded = true;

        Bounds q(bounds);
        for (std::size_t n(0); n < 3; ++n)
        {
            q = q.get(toDir((depth + n) % 8));
            const std::size_t np(r.query(q, depth).size() / schema.pointSize());

            // const Bounds s(q.deltify(delta));
            ASSERT_EQ(np, o.query(q, depth).size()) <<
                " B: " << bounds << " D: " << depth << std::endl;
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

    Expectations one(single, actualBounds);
    Expectations two(multi, actualBounds);
    Expectations con(continued, actualBounds);

    INSTANTIATE_TEST_CASE_P(
            Absolute,
            BuildTest,
            testing::Values(one, two, con), );
}

namespace scaled
{
    Json::Value single(([]()
    {
        Json::Value json;
        json["input"] = test::dataPath() + "ellipsoid-single-laz";
        json["output"] = outPath;
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

    const Delta delta(Scale(.01));
    const Bounds actualScaledBounds(
            actualBounds.scale(delta.scale(), delta.offset()));

    Expectations one(single, actualBounds, delta);
    Expectations two(multi, actualBounds, delta);
    Expectations con(continued, actualBounds, delta);

    INSTANTIATE_TEST_CASE_P(
            Scaled,
            BuildTest,
            testing::Values(one, two, con), );
}

TEST(Build, Kernel)
{
    std::string output;
    int status(0);

    status = run("build");
    EXPECT_EQ(status, 0);

    // TODO Test other CLI invocations.
}


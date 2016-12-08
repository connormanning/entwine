#include "gtest/gtest.h"
#include "config.hpp"

#include <pdal/Dimension.hpp>
#include <pdal/util/FileUtils.hpp>

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
    const auto& config(expect.config);

    auto builder(ConfigParser::getBuilder(config));
    ASSERT_TRUE(builder);
    ASSERT_FALSE(builder->isContinuation());
    builder->go();

    const std::string input(
            arbiter::util::getBasename(config["input"].asString()));

    const Json::Value meta(parse(outEp.get("entwine")));

    const Metadata metadata(meta);
    const Manifest manifest(parse(outEp.get("entwine-manifest")));
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
    const Bounds boundsNative(meta["boundsNative"]);

    EXPECT_EQ(boundsConforming, expect.boundsConforming);

    EXPECT_TRUE(bounds.isCubic());
    EXPECT_TRUE(bounds.contains(boundsConforming));

    EXPECT_EQ(
            boundsNative.scale(delta.scale(), delta.offset()),
            boundsConforming);

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

    test::Octree o(
            bounds,
            delta,
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

    Expectations one(single, actualBounds);
    Expectations two(multi, actualBounds);

    INSTANTIATE_TEST_CASE_P(
            Absolute,
            BuildTest,
            testing::Values(one, two), );
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

    const Delta delta(Scale(.01));
    const Bounds actualScaledBounds(
            actualBounds.scale(delta.scale(), delta.offset()));

    Expectations one(single, actualScaledBounds, delta);
    Expectations two(multi, actualScaledBounds, delta);

    INSTANTIATE_TEST_CASE_P(
            Scaled,
            BuildTest,
            testing::Values(one, two), );
}

TEST(Build, Basic)
{
    Json::Value json;
    json["input"] = test::dataPath() + "ellipsoid-single-laz";
    json["output"] = outPath;
    json["absolute"] = true;

    for (const auto p : a.resolve(outPath + "/**"))
    {
        pdal::FileUtils::deleteFile(p);
    }

    auto builder(ConfigParser::getBuilder(json));
    ASSERT_TRUE(builder);
    ASSERT_FALSE(builder->isContinuation());
    builder->go();

    const Json::Value meta(parse(outEp.get("entwine")));

    const Delta delta(meta);
    /*
    const Scale scale(meta["scale"]);
    const Offset offset(meta["offset"]);
    EXPECT_EQ(scale, Scale(.01));
    EXPECT_EQ(offset, Offset());
    */

    const Bounds boundsNative(meta["boundsNative"]);
    const Bounds boundsConforming(meta["boundsConforming"]);
    const Bounds bounds(meta["bounds"]);

    EXPECT_EQ(
            boundsConforming,
            boundsNative.scale(delta.scale(), delta.offset()));
    EXPECT_EQ(boundsNative, actualBounds);
    EXPECT_TRUE(bounds.contains(boundsConforming));
    EXPECT_TRUE(bounds.isCubic());

    EXPECT_TRUE(meta["compress"].asBool());
    EXPECT_TRUE(meta["trustHeaders"].asBool());
    EXPECT_EQ(meta["compressHierarchy"].asString(), "lzma");

    const Schema schema(meta["schema"]);
    for (const auto d : actualDims)
    {
        EXPECT_TRUE(schema.contains(pdal::Dimension::name(d)));
    }

    test::Octree o(bounds, meta["structure"]["nullDepth"].asUInt64());
    o.insert(test::dataPath() + "ellipsoid-single-laz/ellipsoid.laz");

    Cache cache(32);
    Reader r(outPath, cache);

    ASSERT_EQ(
            r.metadata().manifest().pointStats().inserts(),
            o.inserts());

    bool pointsFound(false);
    bool pointsEnded(false);
    std::size_t depth(0);

    while (!pointsEnded)
    {
        const std::size_t np(r.query(depth).size() / schema.pointSize());
        ASSERT_EQ(np, o.query(depth).size());

        if (np) pointsFound = true;
        if (!np && pointsFound) pointsEnded = true;

        ++depth;
    }
}


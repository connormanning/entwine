#include "gtest/gtest.h"
#include "config.hpp"

#include "entwine/types/reprojection.hpp"
#include "entwine/util/inference.hpp"

using namespace entwine;

namespace
{

const Bounds nominalBounds(-150, -100, -50, 150, 100, 50);
const Point nycCenter(-8242596.036, 4966606.257);
const Bounds nycBounds(
        nycCenter + nominalBounds.min(),
        nycCenter + nominalBounds.max());
const double e(.0001);

void checkPointNear(const Point& a, const Point& b, double err = e)
{
    EXPECT_NEAR(a.x, b.x, err);
    EXPECT_NEAR(a.y, b.y, err);
    EXPECT_NEAR(a.z, b.z, err);
}

void checkBoundsNear(const Bounds& a, const Bounds& b, double err = e)
{
    checkPointNear(a.min(), b.min(), err);
    checkPointNear(a.max(), b.max(), err);
}

void checkCommon(const Inference& inference)
{
    const Schema& schema(inference.schema());
    EXPECT_TRUE(schema.contains("X"));
    EXPECT_TRUE(schema.contains("Y"));
    EXPECT_TRUE(schema.contains("Z"));
    EXPECT_TRUE(schema.contains("Intensity"));
    EXPECT_TRUE(schema.contains("ReturnNumber"));
    EXPECT_TRUE(schema.contains("NumberOfReturns"));
    EXPECT_TRUE(schema.contains("EdgeOfFlightLine"));
    EXPECT_TRUE(schema.contains("Classification"));
    EXPECT_TRUE(schema.contains("ScanAngleRank"));
    EXPECT_TRUE(schema.contains("PointSourceId"));
    EXPECT_TRUE(schema.contains("GpsTime"));
    EXPECT_TRUE(schema.contains("Red"));
    EXPECT_TRUE(schema.contains("Green"));
    EXPECT_TRUE(schema.contains("Blue"));

    EXPECT_EQ(inference.nativeBounds(), nominalBounds);
    EXPECT_EQ(inference.numPoints(), 100000u);
}

void checkDelta(const Inference& inference, const Delta& ver)
{
    const Delta* delta(inference.delta());
    EXPECT_TRUE(delta);

    if (delta)
    {
        EXPECT_EQ(delta->scale(), ver.scale());
        EXPECT_EQ(delta->offset(), ver.offset());
    }
}

} // unnamed namespace

TEST(Infer, Empty)
{
    Inference inference(test::dataPath() + "not-a-real-directory");
    EXPECT_FALSE(inference.done());
    EXPECT_ANY_THROW(inference.go());
}

TEST(Infer, EllipsoidSingleLaz)
{
    Inference inference(test::dataPath() + "ellipsoid-single-laz");
    inference.go();
    ASSERT_TRUE(inference.done());

    checkCommon(inference);
    checkDelta(inference, Delta(Scale(.01), Offset(0)));

    EXPECT_EQ(inference.manifest().size(), 1u);
    EXPECT_FALSE(inference.reprojection());
    EXPECT_FALSE(inference.transformation());
}

TEST(Infer, EllipsoidMultiLaz)
{
    Inference inference(test::dataPath() + "ellipsoid-multi-laz");
    inference.go();
    ASSERT_TRUE(inference.done());

    checkCommon(inference);
    checkDelta(inference, Delta(Scale(.01), Offset(0)));

    EXPECT_EQ(inference.manifest().size(), 8u);
    EXPECT_FALSE(inference.reprojection());
    EXPECT_FALSE(inference.transformation());
}

TEST(Infer, EllipsoidMultiBpf)
{
    Inference inference(test::dataPath() + "ellipsoid-multi-bpf");
    inference.go();
    ASSERT_TRUE(inference.done());

    checkCommon(inference);
    EXPECT_FALSE(inference.delta());

    EXPECT_EQ(inference.manifest().size(), 8u);
    EXPECT_FALSE(inference.reprojection());
    EXPECT_FALSE(inference.transformation());
}

TEST(Infer, Reprojection)
{
    const std::string path(test::dataPath() + "ellipsoid-single-nyc");
    const std::string badPath(path + "-wrong-srs");

    const Bounds utmBounds(
            Point(580621.19214, 4504618.31537, -50),
            Point(580850.55166, 4504772.01557, 50));

    {
        Inference inference(path);
        inference.go();
        ASSERT_TRUE(inference.done());

        checkBoundsNear(inference.nativeBounds(), nycBounds);

        const Delta* d(inference.delta());
        ASSERT_TRUE(d);
        EXPECT_EQ(d->scale(), Scale(.01));
        checkPointNear(d->offset(), nycCenter, 20.0);
    }

    {
        const Reprojection r("", "EPSG:26918");
        Inference inference(path, &r);
        inference.go();
        ASSERT_TRUE(inference.done());

        checkBoundsNear(inference.nativeBounds(), utmBounds);

        const Delta* d(inference.delta());
        ASSERT_TRUE(d);
        EXPECT_EQ(d->scale(), Scale(.01));
        checkPointNear(d->offset(), utmBounds.mid(), 20.0);
    }

    {
        const Reprojection r("EPSG:3857", "EPSG:26918", true);
        Inference inference(badPath, &r);
        inference.go();
        ASSERT_TRUE(inference.done());

        const Delta* d(inference.delta());
        ASSERT_TRUE(d);
        EXPECT_EQ(d->scale(), Scale(.01));
        checkPointNear(d->offset(), utmBounds.mid(), 20.0);
    }
}

TEST(Infer, TrustHeaders)
{
    Inference inference(test::dataPath() + "ellipsoid-multi-nyc");
    inference.go();
    ASSERT_TRUE(inference.done());

    EXPECT_EQ(
            inference.nativeBounds(),
            Bounds(
                nycCenter + nominalBounds.min(),
                nycCenter + nominalBounds.max()));
}


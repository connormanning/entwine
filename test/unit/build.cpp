#include "gtest/gtest.h"
#include "config.hpp"
#include "verify.hpp"

#include <entwine/builder/builder.hpp>

using namespace entwine;
using DimId = pdal::Dimension::Id;

namespace
{
    const arbiter::Arbiter a;
    const Verify v;
}

TEST(build, basic)
{
    const std::string out(test::dataPath() + "out/ellipsoid/");
    Config c;
    c["input"] = test::dataPath() + "ellipsoid.laz";
    c["output"] = out;
    c["force"] = true;
    c["ticks"] = v.ticks();
    c["hierarchyStep"] = v.hierarchyStep();

    Builder b(c);
    b.go();

    const auto info(parse(a.get(out + "entwine.json")));

    const Bounds bounds(info["bounds"]);
    const Bounds boundsConforming(info["boundsConforming"]);
    EXPECT_TRUE(bounds.isCubic());
    EXPECT_TRUE(bounds.contains(boundsConforming));

    const auto dataType(info["dataType"].asString());
    EXPECT_EQ(dataType, "laszip");

    const auto hierarchyType(info["hierarchyType"].asString());
    const auto hierarchyStep(info["hierarchyStep"].asUInt64());
    EXPECT_EQ(hierarchyType, "json");
    EXPECT_EQ(hierarchyStep, 2u);

    const auto numPoints(info["numPoints"].asUInt64());
    EXPECT_EQ(numPoints, v.numPoints());

    const Scale scale(info["scale"]);
    const Offset offset(info["offset"]);
    EXPECT_EQ(scale, v.scale());
    EXPECT_EQ(offset, bounds.mid());

    const Schema schema(info["schema"]);
    EXPECT_EQ(schema, v.schema().append(DimId::OriginId));

    EXPECT_EQ(info["ticks"].asUInt64(), v.ticks());
    EXPECT_EQ(info["hierarchyStep"].asUInt64(), v.hierarchyStep());
}


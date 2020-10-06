#include "gtest/gtest.h"
#include "config.hpp"
#include "verify.hpp"

#include <entwine/types/bounds.hpp>
#include <entwine/util/info.hpp>

using namespace entwine;

namespace
{
    const Verify verify;
}

TEST(info, analyzeFile)
{
    const StringList inputs{test::dataPath() + "ellipsoid.laz"};
    const auto list = analyze(inputs);
    EXPECT_EQ(list.size(), 1);

    const auto item = list.at(0);
    const auto path = item.path;
    const auto info = item.info;

    EXPECT_EQ(path, inputs.at(0));
    EXPECT_EQ(info.errors.size(), 0);
    EXPECT_EQ(info.bounds, verify.bounds);
    EXPECT_EQ(info.points, verify.points);
    EXPECT_EQ(info.srs, verify.srs);
    EXPECT_EQ(info.schema.size(), verify.schema.size());
    for (std::size_t i = 0; i < info.schema.size(); ++i)
    {
        const auto& a = info.schema.at(i);
        const auto& b = verify.schema.at(i);
        EXPECT_EQ(a.name, b.name);
        EXPECT_EQ(a.type, b.type);
        EXPECT_EQ(a.scale, b.scale);
        EXPECT_EQ(a.offset, b.offset);
    }
}

TEST(info, analyzeReprojected)
{
    const std::string utmString = "EPSG:26918";
    const Srs utmSrs(utmString);
    const StringList inputs{test::dataPath() + "ellipsoid.laz"};
    const auto list = analyze(inputs, {
        {},
        {{ "type", "filters.reprojection" }, { "out_srs", utmString}}
    });
    EXPECT_EQ(list.size(), 1);

    const auto item = list.at(0);
    const auto path = item.path;
    const auto info = item.info;

    EXPECT_EQ(path, inputs.at(0));
    EXPECT_EQ(info.errors.size(), 0);
    for (std::size_t i = 0; i < 6u; ++i)
    {
        EXPECT_NEAR(info.bounds[i], verify.boundsUtm[i], 0.01);
    }
    EXPECT_EQ(info.points, verify.points);
    EXPECT_EQ(info.srs, utmSrs);
    EXPECT_EQ(info.schema.size(), verify.schema.size());
    for (std::size_t i = 0; i < info.schema.size(); ++i)
    {
        const auto& a = info.schema.at(i);
        const auto& b = verify.schema.at(i);
        EXPECT_EQ(a.name, b.name);
        EXPECT_EQ(a.type, b.type);
        EXPECT_EQ(a.scale, b.scale);
        EXPECT_EQ(a.offset, b.offset);
    }
}

TEST(info, analyzeDirectory)
{
    // This directory consists of precisely the same data as ellipsoid.laz, but
    // split into octants.  There is also a non-point-cloud file zzz.txt which
    // should be noted as an error without affecting the results.
    const StringList inputs{test::dataPath() + "ellipsoid-multi"};
    const auto list = analyze(inputs);

    EXPECT_EQ(list.size(), 9);

    const auto info = manifest::reduce(list);

    EXPECT_EQ(info.errors.size(), 1);
    EXPECT_EQ(info.bounds, verify.bounds);
    EXPECT_EQ(info.points, verify.points);
    EXPECT_EQ(info.srs, verify.srs);
    EXPECT_EQ(info.schema.size(), verify.schema.size());
    for (std::size_t i = 0; i < info.schema.size(); ++i)
    {
        const auto& a = info.schema.at(i);
        const auto& b = verify.schema.at(i);
        EXPECT_EQ(a.name, b.name);
        EXPECT_EQ(a.type, b.type);
        EXPECT_EQ(a.scale, b.scale);
        EXPECT_EQ(a.offset, b.offset);
    }
}

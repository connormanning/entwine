#include "gtest/gtest.h"

#include <entwine/types/srs.hpp>

using namespace entwine;

TEST(srs, empty)
{
    Srs srs;
    EXPECT_TRUE(srs.empty());
    EXPECT_TRUE(srs.authority().empty());
    EXPECT_TRUE(srs.horizontal().empty());
    EXPECT_TRUE(srs.vertical().empty());
    EXPECT_TRUE(srs.wkt().empty());
    const json j(srs);
    EXPECT_TRUE(j.is_object());
    EXPECT_TRUE(keys(j).empty());
}

TEST(srs, emptyString)
{
    Srs srs(std::string(""));
    EXPECT_TRUE(srs.empty());
    EXPECT_TRUE(srs.authority().empty());
    EXPECT_TRUE(srs.horizontal().empty());
    EXPECT_TRUE(srs.vertical().empty());
    EXPECT_TRUE(srs.wkt().empty());
    const json j(srs);
    EXPECT_TRUE(j.is_object());
    EXPECT_TRUE(keys(j).empty());
}

TEST(srs, fromHorizontalCode)
{
    const std::string s("EPSG:26915");
    Srs srs(s);
    pdal::SpatialReference ref(s);

    EXPECT_FALSE(srs.empty());
    EXPECT_EQ(srs.authority(), "EPSG");
    EXPECT_EQ(srs.horizontal(), "26915");
    EXPECT_TRUE(srs.vertical().empty());
    EXPECT_EQ(srs.wkt(), ref.getWKT());

    const json j(srs);
    const json v {
        { "authority", "EPSG" },
        { "horizontal", "26915" },
        { "wkt", ref.getWKT() }
    };
    EXPECT_EQ(j, v) << j.dump(2) << " != " << v.dump(2) << std::endl;
}

TEST(srs, fromCompoundCode)
{
    const std::string s("EPSG:26915+5703");
    Srs srs(s);
    pdal::SpatialReference ref(s);

    EXPECT_FALSE(srs.empty());
    EXPECT_EQ(srs.authority(), "EPSG");
    EXPECT_EQ(srs.horizontal(), "26915");
    EXPECT_EQ(srs.vertical(), "5703");
    EXPECT_EQ(srs.wkt(), ref.getWKT());

    const json j(srs);
    const json v {
        { "authority", "EPSG" },
        { "horizontal", "26915" },
        { "vertical", "5703" },
        { "wkt", ref.getWKT() }
    };
    EXPECT_EQ(j, v) << j.dump(2) << " != " << v.dump(2) << std::endl;
}

TEST(srs, fromHorizontalWkt)
{
    const std::string s("EPSG:26915");
    pdal::SpatialReference ref(s);
    Srs srs(ref.getWKT());

    EXPECT_FALSE(srs.empty());
    EXPECT_EQ(srs.authority(), "EPSG");
    EXPECT_EQ(srs.horizontal(), "26915");
    EXPECT_TRUE(srs.vertical().empty());
    EXPECT_EQ(srs.wkt(), ref.getWKT());

    const json j(srs);
    const json v {
        { "authority", "EPSG" },
        { "horizontal", "26915" },
        { "wkt", ref.getWKT() }
    };
    EXPECT_EQ(j, v) << j.dump(2) << " != " << v.dump(2) << std::endl;
}

TEST(srs, fromCompoundWkt)
{
    // Unfortunately GDAL's auto-identify-vertical-EPSG doesn't actually return
    // the vertical in this case.
    const std::string s("EPSG:26915+5703");
    pdal::SpatialReference ref(s);
    Srs srs(ref.getWKT());

    EXPECT_FALSE(srs.empty());
    EXPECT_EQ(srs.authority(), "EPSG");
    EXPECT_EQ(srs.horizontal(), "26915");
    EXPECT_EQ(srs.vertical(), "");
    EXPECT_EQ(srs.wkt(), ref.getWKT());

    const json j(srs);
    const json v {
        { "authority", "EPSG" },
        { "horizontal", "26915" },
        { "wkt", ref.getWKT() }
    };
    EXPECT_EQ(j, v) << j.dump(2) << " != " << v.dump(2) << std::endl;
}

TEST(srs, fromJson)
{
    const std::string s("EPSG:26915+5703");
    pdal::SpatialReference ref(s);

    json in {
        { "authority", "EPSG" },
        { "horizontal", "26915" },
        { "vertical", "5703" },
        { "wkt", ref.getWKT() }
    };

    Srs srs(in);

    EXPECT_FALSE(srs.empty());
    EXPECT_EQ(srs.authority(), "EPSG");
    EXPECT_EQ(srs.horizontal(), "26915");
    EXPECT_EQ(srs.vertical(), "5703");
    EXPECT_EQ(srs.wkt(), ref.getWKT());

    const json j(srs);
    const json v {
        { "authority", "EPSG" },
        { "horizontal", "26915" },
        { "vertical", "5703" },
        { "wkt", ref.getWKT() }
    };
    EXPECT_EQ(j, v) << j.dump(2) << " != " << v.dump(2) << std::endl;
}


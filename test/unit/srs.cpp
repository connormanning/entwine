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
    EXPECT_TRUE(srs.toJson().isNull());
}

TEST(srs, emptyString)
{
    Srs srs(std::string(""));
    EXPECT_TRUE(srs.empty());
    EXPECT_TRUE(srs.authority().empty());
    EXPECT_TRUE(srs.horizontal().empty());
    EXPECT_TRUE(srs.vertical().empty());
    EXPECT_TRUE(srs.wkt().empty());
    EXPECT_TRUE(srs.toJson().isNull());
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

    const Json::Value json(srs.toJson());
    EXPECT_EQ(json["authority"].asString(), "EPSG");
    EXPECT_EQ(json["horizontal"].asString(), "26915");
    EXPECT_TRUE(json["vertical"].isNull());
    EXPECT_EQ(json["wkt"].asString(), ref.getWKT());
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

    const Json::Value json(srs.toJson());
    EXPECT_EQ(json["authority"].asString(), "EPSG");
    EXPECT_EQ(json["horizontal"].asString(), "26915");
    EXPECT_EQ(json["vertical"].asString(), "5703");
    EXPECT_EQ(json["wkt"].asString(), ref.getWKT());
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

    const Json::Value json(srs.toJson());
    EXPECT_EQ(json["authority"].asString(), "EPSG");
    EXPECT_EQ(json["horizontal"].asString(), "26915");
    EXPECT_TRUE(json["vertical"].isNull());
    EXPECT_EQ(json["wkt"].asString(), ref.getWKT());
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

    const Json::Value json(srs.toJson());
    EXPECT_EQ(json["authority"].asString(), "EPSG");
    EXPECT_EQ(json["horizontal"].asString(), "26915");
    EXPECT_TRUE(json["vertical"].isNull());
    EXPECT_EQ(json["wkt"].asString(), ref.getWKT());
}

TEST(srs, fromJson)
{
    const std::string s("EPSG:26915+5703");
    pdal::SpatialReference ref(s);

    Json::Value in;
    in["authority"] = "EPSG";
    in["horizontal"] = "26915";
    in["vertical"] = "5703";
    in["wkt"] = ref.getWKT();

    Srs srs(in);

    EXPECT_FALSE(srs.empty());
    EXPECT_EQ(srs.authority(), "EPSG");
    EXPECT_EQ(srs.horizontal(), "26915");
    EXPECT_EQ(srs.vertical(), "5703");
    EXPECT_EQ(srs.wkt(), ref.getWKT());

    const Json::Value json(srs.toJson());
    EXPECT_EQ(json["authority"].asString(), "EPSG");
    EXPECT_EQ(json["horizontal"].asString(), "26915");
    EXPECT_EQ(json["vertical"].asString(), "5703");
    EXPECT_EQ(json["wkt"].asString(), ref.getWKT());
}


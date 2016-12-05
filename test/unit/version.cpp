#include "gtest/gtest.h"

#include <entwine/types/version.hpp>

using namespace entwine;

namespace
{
}

TEST(Version, Basic)
{
    {
        const Version v(1);
        EXPECT_EQ(v.major(), 1);
        EXPECT_EQ(v.minor(), 0);
        EXPECT_EQ(v.patch(), 0);
    }
    {
        const Version v(2, 3);
        EXPECT_EQ(v.major(), 2);
        EXPECT_EQ(v.minor(), 3);
        EXPECT_EQ(v.patch(), 0);
    }
    {
        const Version v(4, 5, 42);
        EXPECT_EQ(v.major(), 4);
        EXPECT_EQ(v.minor(), 5);
        EXPECT_EQ(v.patch(), 42);
    }
}

TEST(Version, String)
{
    {
        const Version v("1");
        EXPECT_EQ(v.major(), 1);
        EXPECT_EQ(v.minor(), 0);
        EXPECT_EQ(v.patch(), 0);
    }
    {
        const Version v("2.");
        EXPECT_EQ(v.major(), 2);
        EXPECT_EQ(v.minor(), 0);
        EXPECT_EQ(v.patch(), 0);
    }
    {
        const Version v("3.4");
        EXPECT_EQ(v.major(), 3);
        EXPECT_EQ(v.minor(), 4);
        EXPECT_EQ(v.patch(), 0);
    }
    {
        const Version v("10.6.22");
        EXPECT_EQ(v.major(), 10);
        EXPECT_EQ(v.minor(), 6);
        EXPECT_EQ(v.patch(), 22);
    }

    EXPECT_ANY_THROW(Version("1.s"));
    EXPECT_ANY_THROW(Version("1.0.1s"));
}

TEST(Version, Comparison)
{
    EXPECT_TRUE(Version(1, 0, 0) < Version(2, 0, 0));
    EXPECT_TRUE(Version(1, 0, 0) < Version(1, 0, 1));
    EXPECT_TRUE(Version(1, 0, 0) < Version(1, 1, 0));
    EXPECT_TRUE(Version(2, 0, 0) >= Version(1, 0, 0));
    EXPECT_TRUE(Version(2, 3, 0) >= Version(2, 0, 9));
    EXPECT_TRUE(Version(2, 3, 3) >= Version(2, 3, 3));
}


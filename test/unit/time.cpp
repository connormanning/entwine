#include "gtest/gtest.h"

#include <thread>

#include <entwine/util/time.hpp>

using namespace entwine;

TEST(time, since)
{
    const auto start = now();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_GE(since<std::chrono::milliseconds>(start), 20);
}

TEST(time, format)
{
    EXPECT_EQ(formatTime(0), "00:00");
    EXPECT_EQ(formatTime(1), "00:01");
    EXPECT_EQ(formatTime(60), "01:00");
    EXPECT_EQ(formatTime(60), "01:00");
    EXPECT_EQ(formatTime(3601), "01:00:01");
}

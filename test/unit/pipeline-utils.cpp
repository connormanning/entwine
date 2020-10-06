#include "gtest/gtest.h"

#include <thread>

#include <entwine/util/pipeline.hpp>

using namespace entwine;

namespace
{
    const json p {
        { { "type", "readers.ept" } },
        { { "type", "filters.smrf" }, { "foo", "bar" } }
    };
}

TEST(pipeline, findStage)
{
    const auto smrf = findStage(p, "filters.smrf");
    EXPECT_NE(smrf, p.end());
    EXPECT_EQ(smrf->at("foo").get<std::string>(), "bar");
    EXPECT_EQ(findStage(p, "filters.asdf"), p.end());
}

TEST(pipeline, findOrAppendStage)
{
    auto mut = p;

    // This stage already exists, so this is a noop.
    auto& smrf = findOrAppendStage(mut, "filters.smrf");
    EXPECT_EQ(smrf.at("foo").get<std::string>(), "bar");
    EXPECT_EQ(mut, p);

    // This one is appended.
    auto& stats = findOrAppendStage(mut, "filters.stats");
    EXPECT_EQ(stats, json::object({ { "type", "filters.stats" } }));
    auto verify = p;
    verify.push_back(stats);
    EXPECT_EQ(mut, verify);
}

TEST(pipeline, omitStage)
{
    EXPECT_EQ(omitStage(p, "filters.asdf"), p);
    EXPECT_EQ(
        omitStage(p, "filters.smrf"),
        json({ { { "type", "readers.ept" } } }));
}

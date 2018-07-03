#include "gtest/gtest.h"

#include "config.hpp"
#include "verify.hpp"

#include <entwine/builder/builder.hpp>
#include <entwine/new-reader/new-reader.hpp>

namespace
{
    const Verify v;
}

TEST(read, basic)
{
    const std::string out(test::dataPath() + "out/ellipsoid/ellipsoid-multi");

    {
        Config c;
        c["input"] = test::dataPath() + "ellipsoid.laz";
        c["output"] = out;
        c["force"] = true;
        c["hierarchyStep"] = static_cast<Json::UInt64>(v.hierarchyStep());
        c["ticks"] = static_cast<Json::UInt64>(v.ticks());

        Builder b(c);
        b.go();
    }

    NewReader r(out);
    const Metadata& m(r.metadata());
    EXPECT_EQ(m.ticks(), v.ticks());
    EXPECT_EQ(m.hierarchyStep(), v.hierarchyStep());

    Json::Value q;

    auto count = r.count(q);
    count->run();
    const uint64_t np(count->numPoints());

    EXPECT_EQ(np, v.numPoints());
}


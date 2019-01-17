#include "gtest/gtest.h"

#include "config.hpp"
#include "verify.hpp"

#include <entwine/builder/builder.hpp>
#include <entwine/reader/reader.hpp>

namespace
{
    const Verify v;
}

TEST(read, count)
{
    const std::string out(test::dataPath() + "out/ellipsoid/ellipsoid");

    {
        Config c(json {
            { "input", test::dataPath() + "ellipsoid.laz" },
            { "output", out },
            { "force", true },
            { "hierarchyStep", v.hierarchyStep() },
            { "span", v.span() }
        });

        Builder b(c);
        b.go();
    }

    Reader r(out);
    const Metadata& m(r.metadata());
    EXPECT_EQ(m.span(), v.span());

    uint64_t np(0);
    for (std::size_t i(0); i < 8; ++i)
    {
        json q { { "bounds", m.boundsCubic().get(toDir(i)) } };

        auto countQuery = r.count(q);
        countQuery->run();
        np += countQuery->points();
    }

    EXPECT_EQ(np, v.points());
}

TEST(read, data)
{
    const std::string out(test::dataPath() + "out/ellipsoid/ellipsoid");

    {
        Config c(json {
            { "input", test::dataPath() + "ellipsoid.laz" },
            { "output", out },
            { "force", true },
            { "hierarchyStep", v.hierarchyStep() },
            { "span", v.span() }
        });

        Builder b(c);
        b.go();
    }

    Reader r(out);
    const Metadata& m(r.metadata());
    EXPECT_EQ(m.span(), v.span());

    const Schema schema(DimList { DimId::X, DimId::Y, DimId::Z });

    auto append([&r](std::vector<char>& v, json j)
    {
        auto q(r.read(j));
        q->run();
        v.insert(v.end(), q->data().begin(), q->data().end());
    });

    json j{ {"schema", schema } };

    std::vector<char> bin;
    for (std::size_t i(0); i < 8; ++i)
    {
        j["bounds"] = m.boundsCubic().get(toDir(i));
        append(bin, j);
    }

    struct Cmp
    {
        bool operator()(const Point& a, const Point& b) const
        {
            return ltChained(a, b);
        }
    };
    using Counts = std::map<Point, uint64_t, Cmp>;

    auto count([&schema](const std::vector<char>& v) -> Counts
    {
        Counts c;
        const std::size_t size(sizeof(double));
        for (std::size_t i(0); i < v.size(); i += schema.pointSize())
        {
            Point p;

            const char* pos(v.data() + i);
            std::copy(pos, pos + size, reinterpret_cast<char*>(&p.x));
            pos += size;
            std::copy(pos, pos + size, reinterpret_cast<char*>(&p.y));
            pos += size;
            std::copy(pos, pos + size, reinterpret_cast<char*>(&p.z));

            if (!c.count(p)) c[p] = 1;
            else ++c[p];
        }

        return c;
    });

    const Counts counts(count(bin));

    ASSERT_EQ(counts.size(), v.points());
}

TEST(read, filter)
{
}


#include "gtest/gtest.h"
#include "config.hpp"

#include <entwine/third/arbiter/arbiter.hpp>

TEST(init, initializes)
{
    const arbiter::Arbiter a;
    if (!a.tryGetSize(test::dataPath() + "ellipsoid.laz"))
    {
        std::cout << "Downloading test data..." << std::endl;

        const std::string base("https://github.com/PDAL/data/raw/master/entwine/test/");
        const std::vector<std::string> files {
            "ellipsoid-multi/ned.laz",
            "ellipsoid-multi/neu.laz",
            "ellipsoid-multi/nwd.laz",
            "ellipsoid-multi/nwu.laz",
            "ellipsoid-multi/sed.laz",
            "ellipsoid-multi/seu.laz",
            "ellipsoid-multi/swd.laz",
            "ellipsoid-multi/swu.laz",
            "ellipsoid-multi/zzz.txt",
            "ellipsoid-wrong-srs.laz",
            "ellipsoid.laz"
        };

        arbiter::mkdirp(test::dataPath());
        arbiter::mkdirp(test::dataPath() + "ellipsoid-multi");
        for (const std::string& path : files)
        {
            a.copy(base + path, test::dataPath() + path, true);
        }
    }

    EXPECT_TRUE(a.tryGetSize(test::dataPath() + "ellipsoid.laz"));
}


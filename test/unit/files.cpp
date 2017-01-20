#include "gtest/gtest.h"
#include "config.hpp"

#include <pdal/util/FileUtils.hpp>

#include <entwine/reader/reader.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/config-parser.hpp>

using namespace entwine;

namespace
{
    const std::string outPath(test::dataPath() + "out/");
    arbiter::Arbiter a;

    const std::vector<std::string> order{
        "ned.laz", "neu.laz", "nwd.laz", "nwu.laz",
        "sed.laz", "seu.laz", "swd.laz", "swu.laz"
    };

    std::string basename(std::string path)
    {
        return arbiter::util::getBasename(path);
    }

    class Matches
    {
    public:
        Matches(Paths paths) : m_paths(paths) { }

        bool good(const FileInfoList& check)
        {
            m_list = check;
            std::set<std::string> set(m_paths.begin(), m_paths.end());

            for (const auto& fileInfo : check)
            {
                const auto path(basename(fileInfo.path()));
                if (set.count(path)) set.erase(path);
                else return false;
            }

            return true;
        }

        std::string message() const
        {
            std::string s;
            s += "Got: ";
            for (const auto& f : m_list) s += basename(f.path()) + " ";
            s += "\nWanted: ";
            for (const auto& p : m_paths) s += p + " ";
            s += "\n";
            return s;
        }

        const FileInfoList& list() const { return m_list; }
        const Paths& paths() const { return m_paths; }
        std::size_t size() const { return m_paths.size(); }

    private:
        const Paths m_paths;
        FileInfoList m_list;
    };
}

class FilesTest : public ::testing::Test
{
protected:
    FilesTest() : cache(5000) { }

    virtual void SetUp()
    {
        {
            Json::Value config;
            config["input"] = test::dataPath() + "ellipsoid-multi-laz";
            config["output"] = outPath + "f";

            auto builder(ConfigParser::getBuilder(config));
            builder->go();
        }

        reader = makeUnique<Reader>(outPath + "f", cache);
    }

    virtual void TearDown()
    {
        for (const auto p : a.resolve(outPath + "/**"))
        {
            pdal::FileUtils::deleteFile(p);
        }
    }

    Cache cache;
    std::unique_ptr<Reader> reader;
};

TEST_F(FilesTest, SingleOrigin)
{
    for (Origin i(0); i < 8; ++i)
    {
        ASSERT_EQ(basename(reader->files(i).path()), order[i]);
    }

    EXPECT_ANY_THROW(reader->files(8));
}

TEST_F(FilesTest, MultiOrigin)
{
    const std::vector<Origin> origins{ 0, 2, 4, 6 };
    const auto files(reader->files(origins));

    ASSERT_EQ(origins.size(), files.size());

    for (std::size_t i(0); i < files.size(); ++i)
    {
        ASSERT_EQ(basename(files[i].path()), order[origins[i]]);
    }

    std::vector<Origin> badOrigins{ 0, 8 };
    EXPECT_ANY_THROW(reader->files(badOrigins));
}

TEST_F(FilesTest, SingleSearch)
{
    for (const auto search : order)
    {
        ASSERT_EQ(basename(reader->files(search).path()), search);
    }

    EXPECT_ANY_THROW(reader->files(std::string("asdf")));
}

TEST_F(FilesTest, MultiSearch)
{
    const std::vector<std::string> searches{
        "sed.laz", "swu.laz", "neu.laz", "nwd.laz"
    };
    const auto files(reader->files(searches));

    ASSERT_EQ(searches.size(), files.size());

    for (std::size_t i(0); i < files.size(); ++i)
    {
        ASSERT_EQ(basename(files[i].path()), searches[i]);
    }

    std::vector<Origin> badSearches{ "ned.laz", "asdf" };
    EXPECT_ANY_THROW(reader->files(badSearches));
}

TEST_F(FilesTest, Bounds)
{
    {
        const Bounds up(-5, -5, 0, 5, 5, 5);
        const auto files(reader->files(up));
        Matches matches(Paths{ "neu.laz", "nwu.laz", "seu.laz", "swu.laz" });
        EXPECT_TRUE(matches.good(files)) << matches.message();
    }

    {
        const Bounds southwest(-5, -5, 0, 0);
        const auto files(reader->files(southwest));
        Matches matches(Paths{ "swu.laz", "swd.laz" });
        EXPECT_TRUE(matches.good(files)) << matches.message();
    }
}

TEST_F(FilesTest, Filter)
{
    {
        Json::Value filter;

        // Up or east - everything but west-down.
        Json::Value zFilter; zFilter["Z"]["$gt"] = 0;
        Json::Value xFilter; xFilter["X"]["$gt"] = 0;

        auto& orJson(filter["$or"]);
        orJson.append(zFilter); orJson.append(xFilter);

        const auto files(reader->files(filter));
        Matches matches(Paths{
            "ned.laz", "neu.laz", "nwu.laz",
            "sed.laz", "seu.laz", "swu.laz"
        });
        EXPECT_TRUE(matches.good(files)) << matches.message();
    }

    {
        Json::Value filter;
        filter["Path"]["$in"].append("swd.laz");
        filter["Path"]["$in"].append("swu.laz");

        const auto files(reader->files(filter));
        Matches matches(Paths{ "swd.laz", "swu.laz" });
        EXPECT_TRUE(matches.good(files)) << matches.message();
    }

    {
        Json::Value pathsIn;
        pathsIn["Path"]["$in"].append("swd.laz");
        pathsIn["Path"]["$in"].append("swu.laz");

        Json::Value xFilter; xFilter["X"]["$gt"] = 0;
        Json::Value yFilter; yFilter["Y"]["$gt"] = 0;
        Json::Value zFilter; zFilter["Z"]["$gt"] = 0;

        Json::Value neu;
        neu["$and"].append(xFilter);
        neu["$and"].append(yFilter);
        neu["$and"].append(zFilter);

        Json::Value filter;
        auto& orJson(filter["$or"]);
        orJson.append(pathsIn);
        orJson.append(neu);

        const auto files(reader->files(filter));
        Matches matches(Paths{ "swd.laz", "swu.laz", "neu.laz" });
        EXPECT_TRUE(matches.good(files)) << matches.message();
    }
}

TEST_F(FilesTest, FilterWithBounds)
{
    {
        // South only.
        const Bounds bounds(-5, -5, -5, 5, 0, 5);

        // Up or east - everything but west-down.
        Json::Value zFilter; zFilter["Z"]["$gt"] = 0;
        Json::Value xFilter; xFilter["X"]["$gt"] = 0;

        Json::Value filter;
        auto& orJson(filter["$or"]);
        orJson.append(zFilter); orJson.append(xFilter);

        const auto files(reader->files(bounds, filter));
        Matches matches(Paths{ "sed.laz", "seu.laz", "swu.laz" });
        EXPECT_TRUE(matches.good(files)) << matches.message();
    }

    {
        // Up only.
        const Bounds bounds(-5, -5, 0, 5, 5, 5);

        Json::Value filter;
        filter["Path"]["$in"].append("swd.laz");
        filter["Path"]["$in"].append("swu.laz");

        const auto files(reader->files(bounds, filter));
        Matches matches(Paths{ "swu.laz" });
        EXPECT_TRUE(matches.good(files)) << matches.message();
    }

    {
        // South-west-up only.
        const Bounds bounds(-5, -5, 0, 0, 0, 5);

        Json::Value pathsIn;
        pathsIn["Path"]["$in"].append("swd.laz");
        pathsIn["Path"]["$in"].append("swu.laz");

        Json::Value xFilter; xFilter["X"]["$gt"] = 0;
        Json::Value yFilter; yFilter["Y"]["$gt"] = 0;
        Json::Value zFilter; zFilter["Z"]["$gt"] = 0;

        Json::Value neu;
        neu["$and"].append(xFilter);
        neu["$and"].append(yFilter);
        neu["$and"].append(zFilter);

        Json::Value filter;
        auto& orJson(filter["$or"]);
        orJson.append(pathsIn);
        orJson.append(neu);

        const auto files(reader->files(bounds, filter));
        Matches matches(Paths{ "swu.laz" });
        EXPECT_TRUE(matches.good(files)) << matches.message();
    }
}


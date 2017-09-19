#include "gtest/gtest.h"
#include "config.hpp"

#include <pdal/util/FileUtils.hpp>

#include <entwine/reader/reader.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/config-parser.hpp>

using namespace entwine;

namespace
{
    const std::string outPath(test::dataPath() + "out/");
    arbiter::Arbiter a;

    std::string basename(std::string path)
    {
        return arbiter::util::getBasename(path);
    }

    const Point nycCenter(-8242596.04, 4966606.26);

    class Matches
    {
    public:
        Matches(Paths paths) : m_paths(paths) { }

        bool good(const FileInfoList& check)
        {
            m_list = check;
            if (m_list.size() != m_paths.size()) return false;

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
    static void SetUpTestCase()
    {
        {
            Json::Value config;
            config["input"] = test::dataPath() + "ellipsoid-multi-laz";
            config["output"] = outPath + "f";
            config["force"] = true;

            auto builder(ConfigParser::getBuilder(config));
            builder->go();
        }

        {
            Json::Value config;
            config["input"] = test::dataPath() + "ellipsoid-multi-nyc";
            config["output"] = outPath + "n";
            config["force"] = true;

            auto builder(ConfigParser::getBuilder(config));
            builder->go();
        }

        cache = makeUnique<Cache>(5000);
        reader = makeUnique<Reader>(outPath + "f", *cache);
        nycReader = makeUnique<Reader>(outPath + "n", *cache);
    }

    static void TearDownTestCase()
    {
        for (const auto p : a.resolve(outPath + "/**"))
        {
            pdal::FileUtils::deleteFile(p);
        }
    }

    static std::unique_ptr<Cache> cache;
    static std::unique_ptr<Reader> reader;
    static std::unique_ptr<Reader> nycReader;
};

std::unique_ptr<Cache> FilesTest::cache = nullptr;
std::unique_ptr<Reader> FilesTest::reader = nullptr;
std::unique_ptr<Reader> FilesTest::nycReader = nullptr;

TEST_F(FilesTest, SingleOrigin)
{
    for (Origin i(0); i < 8; ++i)
    {
        ASSERT_EQ(
                basename(reader->files(i).path()),
                basename(reader->metadata().manifest().get(i).path()));

        ASSERT_EQ(
                basename(nycReader->files(i).path()),
                basename(nycReader->metadata().manifest().get(i).path()));
    }

    EXPECT_ANY_THROW(reader->files(8));
    EXPECT_ANY_THROW(nycReader->files(8));
}

TEST_F(FilesTest, MultiOrigin)
{
    const std::vector<Origin> origins{ 0, 2, 4, 6 };
    const auto files(reader->files(origins));

    ASSERT_EQ(origins.size(), files.size());

    for (std::size_t i(0); i < files.size(); ++i)
    {
        ASSERT_EQ(
                basename(files[i].path()),
                basename(reader->metadata().manifest().get(origins[i]).path()));
    }

    std::vector<Origin> badOrigins{ 0, 8 };
    EXPECT_ANY_THROW(reader->files(badOrigins));
}

TEST_F(FilesTest, SingleSearch)
{
    const std::vector<std::string> files{
        "ned.laz", "neu.laz", "nwd.laz", "nwu.laz",
        "sed.laz", "seu.laz", "swd.laz", "swu.laz"
    };

    for (const auto search : files)
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

    std::vector<std::string> badSearches{ "ned.laz", "asdf" };
    EXPECT_ANY_THROW(reader->files(badSearches));
}

TEST_F(FilesTest, Bounds)
{
    {
        const Bounds up(-5, -5, 1, 5, 5, 5);
        const auto files(reader->files(up));
        Matches matches(Paths{ "neu.laz", "nwu.laz", "seu.laz", "swu.laz" });
        EXPECT_TRUE(matches.good(files)) << matches.message();
    }

    {
        const Bounds southwest(-5, -5, -1, -1);
        const auto files(reader->files(southwest));
        Matches matches(Paths{ "swu.laz", "swd.laz" });
        EXPECT_TRUE(matches.good(files)) << matches.message();
    }
}

TEST_F(FilesTest, BoundsScaled)
{
    const Scale scale(.01, .1, .0025);
    const Offset offset(314159, 271828, 42);

    {
        const Bounds b(-5, -5, 1, 5, 5, 5);
        const Bounds up = (b + nycCenter).scale(scale, offset);
        const auto files(nycReader->files(up, &scale, &offset));
        Matches matches(Paths{ "neu.laz", "nwu.laz", "seu.laz", "swu.laz" });
        EXPECT_TRUE(matches.good(files)) << matches.message();
    }

    {
        const Bounds b(-5, -5, -1, -1);
        const Bounds southwest = (b + nycCenter).scale(scale, offset);
        const auto files(nycReader->files(southwest, &scale, &offset));
        Matches matches(Paths{ "swu.laz", "swd.laz" });
        EXPECT_TRUE(matches.good(files)) << matches.message();
    }
}


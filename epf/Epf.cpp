#include "Epf.hpp"
#include "EpfTypes.hpp"
#include "FileProcessor.hpp"
#include "Cell.hpp"
#include "Writer.hpp"

#include <unordered_set>

#include <pdal/Dimension.hpp>
#include <pdal/PointLayout.hpp>
#include <pdal/StageFactory.hpp>
#include <pdal/util/Bounds.hpp>
#include <pdal/util/FileUtils.hpp>
#include <pdal/util/ProgramArgs.hpp>

using namespace pdal;

int main(int argc, char *argv[])
{
    std::vector<std::string> arglist;

    // Skip the program name.
    argv++;
    argc--;
    while (argc--)
        arglist.push_back(*argv++);

    epf::Epf preflight;
    preflight.run(arglist);

    return 0;
}

namespace epf
{

std::string indexToString(int index)
{
    uint8_t z = index & 0xFF;
    uint8_t y = (index >> CHAR_BIT) & 0xFF;
    uint8_t x = (index >> (2 * CHAR_BIT)) & 0xFF;
    return std::to_string(x) + "_" + std::to_string(y) + "_" + std::to_string(z);
}

int toIndex(int x, int y, int z)
{
    assert(x < std::numeric_limits<uint8_t>::max());
    assert(y < std::numeric_limits<uint8_t>::max());
    assert(z < std::numeric_limits<uint8_t>::max());
    return (x << (2 * CHAR_BIT)) | (y << CHAR_BIT) | z;
}

/// Epf

Epf::Epf() : m_pool(6)
{}

void Epf::addArgs(ProgramArgs& programArgs)
{
    programArgs.add("output_dir", "Output directory", m_outputDir).setPositional();
    programArgs.add("files", "Files to preflight", m_files).setPositional();
    programArgs.add("file_limit", "Max number of files to process", m_fileLimit, (size_t)10000000);
}

void Epf::run(const std::vector<std::string>& options)
{
    double millionPoints = 0;
    BOX3D totalBounds;
    ProgramArgs programArgs;

    addArgs(programArgs);
    try
    {
        programArgs.parse(options);
    }
    catch (const pdal::arg_error& err)
    {
        std::cerr << err.what() << "\n";
        return;
    }

    m_writer.reset(new Writer(m_outputDir, 4));

    std::vector<FileInfo> fileInfos = createFileInfo();

    if (fileInfos.size() > m_fileLimit)
        fileInfos.resize(m_fileLimit);

    std::unordered_set<std::string> allDimNames;
    for (const FileInfo& fi : fileInfos)
        for (const FileDimInfo& fdi : fi.dimInfo)
            allDimNames.insert(fdi.name);

    PointLayoutPtr layout(new PointLayout());
    for (const std::string& dimName : allDimNames)
    {
        Dimension::Type type = Dimension::defaultType(Dimension::id(dimName));
        if (type == Dimension::Type::None)
            type = Dimension::Type::Double;
        layout->registerOrAssignDim(dimName, type);
    }
    layout->finalize();

    // Fill in dim info now that the layout is finalized.
    for (FileInfo& fi : fileInfos)
    {
        for (FileDimInfo& di : fi.dimInfo)
        {
            di.dim = layout->findDim(di.name);
            di.type = layout->dimType(di.dim);
            di.offset = layout->dimOffset(di.dim);
        }
    }

    std::vector<std::unique_ptr<FileProcessor>> processors;
    // Add the files to the processing pool
    for (const FileInfo& fi : fileInfos)
    {
        std::unique_ptr<FileProcessor> fp(
            new FileProcessor(fi, layout->pointSize(), m_grid, m_writer.get()));
        std::function<void()> processor = std::bind(&FileProcessor::operator(), fp.get());
        m_pool.add(processor);
        processors.push_back(std::move(fp));
    }

    m_pool.join();
    m_writer->stop();
}

std::vector<FileInfo> Epf::createFileInfo()
{
    std::vector<FileInfo> fileInfos;
    std::vector<std::string> filenames;

    for (std::string& filename : m_files)
    {
        if (FileUtils::isDirectory(filename))
        {
            std::vector<std::string> dirfiles = FileUtils::directoryList(filename);
            filenames.insert(filenames.end(), dirfiles.begin(), dirfiles.end());
        }
        else
            filenames.push_back(filename);
    }

    for (std::string& filename : filenames)
    {
        StageFactory factory;
        std::string driver = factory.inferReaderDriver(filename);
        if (driver.empty())
            throw "Can't infer reader for '" + filename + "'.";
        Stage *s = factory.createStage(driver);
        Options opts;
        opts.add("filename", filename);
        s->setOptions(opts);

        QuickInfo qi = s->preview();

        if (!qi.valid())
            throw "Couldn't get quick info for '" + filename + "'.";

        FileInfo fi;
        fi.bounds = qi.m_bounds;
        for (const std::string& name : qi.m_dimNames)
            fi.dimInfo.push_back(FileDimInfo(name));
        fi.filename = filename;
        fi.driver = driver;
        fileInfos.push_back(fi);

        m_grid.expand(qi.m_bounds, qi.m_pointCount);
    }
    return fileInfos;
}

} // namespace epf

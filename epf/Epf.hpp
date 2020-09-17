#pragma once

#include <map>
#include <vector>

#include <pdal/util/ThreadPool.hpp>

#include "EpfTypes.hpp"
#include "Grid.hpp"

namespace pdal
{
    class ProgramArgs;
}

namespace epf
{

std::string indexToString(int index);
int toIndex(int x, int y, int z);

struct FileInfo;
class Cell;
class Writer;

class Epf
{
public:
    Epf();

    void run(const std::vector<std::string>& options);

private:
    void addArgs(pdal::ProgramArgs& programArgs);
    std::vector<FileInfo> createFileInfo();
    Cell *getCell(int index);

    std::vector<std::string> m_files;
    std::string m_outputDir;
    Grid m_grid;
    std::unique_ptr<Writer> m_writer;
    pdal::ThreadPool m_pool;
    size_t m_fileLimit;
};

} // namespace

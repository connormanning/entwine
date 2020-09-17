#include "EpfTypes.hpp"
#include "Grid.hpp"

#include <map>

// Processes a single input file (FileInfo) and writes data to the Writer.
namespace epf
{

class Cell;
class Writer;

class FileProcessor
{
public:
    FileProcessor(const FileInfo& fi, size_t pointSize, const Grid& grid, Writer *writer);

    Cell *getCell(int index);
    void operator()();

private:
    FileInfo m_fi;
    size_t m_pointSize;
    Grid m_grid;
    std::map<int, std::unique_ptr<Cell>> m_cells;
    Writer *m_writer;
    int m_cnt;

    static int m_totalCnt;
};

} // namespace epf

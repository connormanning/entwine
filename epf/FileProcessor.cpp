#include "Cell.hpp"
#include "FileProcessor.hpp"

#include <pdal/StageFactory.hpp>
#include <pdal/filters/StreamCallbackFilter.hpp>

using namespace pdal;

namespace epf
{

int FileProcessor::m_totalCnt = 0;

FileProcessor::FileProcessor(const FileInfo& fi, size_t pointSize, const Grid& grid,
        Writer *writer) :
    m_fi(fi), m_pointSize(pointSize), m_grid(grid), m_writer(writer), m_cnt(++m_totalCnt)
{}    

void FileProcessor::operator()()
{
    Options opts;
    opts.add("filename", m_fi.filename);

    std::cerr << ("Processing " + m_fi.filename + " - " + std::to_string(m_cnt) + "!\n");

    StageFactory factory;
    Stage *s = factory.createStage(m_fi.driver);
    s->setOptions(opts);

    StreamCallbackFilter f;

    size_t count = 0;
    Cell *cell = getCell(0);
    f.setCallback([this, &count, &cell](PointRef& point)
        {
            Buffer b = cell->buffer();
            for (const FileDimInfo& fdi : m_fi.dimInfo)
            point.getField(reinterpret_cast<char *>(b.data() + fdi.offset),
                    fdi.dim, fdi.type);
            size_t cellIndex = m_grid.index(b.x(), b.y(), b.z());
            if (cellIndex != cell->index())
            {
                cell = getCell(cellIndex);
                cell->copyBuffer(b);
            }
            cell->advance();
            count++;
            return true;
        }
    );
    f.setInput(*s);

    FixedPointTable t(1000);

    f.prepare(t);
    f.execute(t);
    
    std::cerr << ("Done " + m_fi.filename + " - " + std::to_string(m_cnt) + ": " +
        std::to_string(count) + " points!\n");

    for (auto& cp : m_cells)
        cp.second->write();
}

Cell *FileProcessor::getCell(int index)
{
    auto it = m_cells.find(index);
    if (it == m_cells.end())
    {
        std::unique_ptr<Cell> cell(new Cell(index, m_pointSize, m_writer));
        it = m_cells.insert( {index, std::move(cell)} ).first;
    }
    Cell& c = *(it->second);
    return &c;
}

} // namespace

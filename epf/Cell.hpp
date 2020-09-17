#pragma once

#include <cstdint>
#include <cstddef>
#include <memory>

// A cell represents a voxel that contains points. All cells are the same size. A cell has
// a buffer which is filled by points. When the buffer is filled, it's passed to the writer
// and a new buffer is created.
namespace epf
{

class Writer;

//Utterly trivial wrapper around a pointer.
class Buffer
{
public:
    Buffer(uint8_t *data) : m_data(data)
    {}

    uint8_t *data()
        { return m_data; }
    double x() const
        { return *ddata(); }
    double y() const
        { return *(ddata() + 1); }
    double z() const
        { return *(ddata() + 2); }

private:
    double *ddata() const
        { return reinterpret_cast<double *>(m_data); }

    uint8_t *m_data;
};

class Cell
{
public:
    static constexpr size_t BufSize = 4096 * 10;

    Cell(int index, size_t pointSize, Writer *writer) : m_index(index), m_pointSize(pointSize),
        m_writer(writer)
    {
        assert(pointSize < BufSize);
        initialize();
    }

    void initialize();
    Buffer buffer()
        { return Buffer(m_pos); }
    int index() const
     { return m_index; }
    void copyBuffer(Buffer& b)
        { std::copy(b.data(), b.data() + m_pointSize, m_pos); }
    void write();
    void advance();

private:
    std::unique_ptr<std::vector<uint8_t>> m_buf;
    int m_index;
    size_t m_pointSize;
    Writer *m_writer;
    uint8_t *m_pos;
    uint8_t *m_endPos;
};

} // namespace epf

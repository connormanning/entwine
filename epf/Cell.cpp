#include "Cell.hpp"
#include "Writer.hpp"

namespace epf
{

void Cell::initialize()
{
    m_buf.reset(new std::vector<uint8_t>(BufSize));
    m_pos = m_buf->data();
    m_endPos = m_pos + m_pointSize * ((BufSize / m_pointSize) - 1);
}

void Cell::write()
{
    // Resize the buffer so the writer knows how much to write.
    m_buf->resize(m_pos - m_buf->data());
    m_writer->enqueue(m_index, std::move(m_buf));
}

void Cell::advance()
{
    m_pos += m_pointSize;
    if (m_pos > m_endPos)
    {
        write();
        initialize();
    }
}

} // namespace epf

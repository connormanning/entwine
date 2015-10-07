/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/compression/stream.hpp>

#include <algorithm>
#include <stdexcept>

namespace entwine
{

CompressionStream::CompressionStream()
    : m_data()
    , m_index(0)
{ }

CompressionStream::CompressionStream(const std::vector<char>& data)
    : m_data(data)
    , m_index(0)
{ }

void CompressionStream::putBytes(const uint8_t* bytes, const std::size_t length)
{
    const std::size_t startSize(m_data.size());
    m_data.resize(m_data.size() + length);
    std::copy(bytes, bytes + length, m_data.data() + startSize);
}

void CompressionStream::putByte(const uint8_t byte)
{
    m_data.push_back(reinterpret_cast<const char&>(byte));
}

uint8_t CompressionStream::getByte()
{
    return reinterpret_cast<uint8_t&>(m_data.at(m_index++));
}

void CompressionStream::getBytes(uint8_t* bytes, std::size_t length)
{
    if (m_index + length > m_data.size())
    {
        throw std::runtime_error("Too many bytes requested!");
    }

    std::copy(m_data.data() + m_index, m_data.data() + m_index + length, bytes);
    m_index += length;
}

const std::vector<char>& CompressionStream::data() const
{
    return m_data;
}

} // namespace entwine


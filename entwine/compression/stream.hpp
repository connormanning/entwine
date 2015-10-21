/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace entwine
{

class CompressionStream
{
public:
    CompressionStream() : m_data(new std::vector<char>()) { }

    void putBytes(const uint8_t* bytes, std::size_t length)
    {
        const std::size_t startSize(m_data->size());
        m_data->resize(m_data->size() + length);
        std::copy(bytes, bytes + length, m_data->data() + startSize);
    }

    void putByte(uint8_t byte)
    {
        m_data->push_back(reinterpret_cast<const char&>(byte));
    }

    std::unique_ptr<std::vector<char>> data()
    {
        return std::move(m_data);
    }

private:
    std::unique_ptr<std::vector<char>> m_data;
};

class DecompressionStream
{
public:
    DecompressionStream(const std::vector<char>& data)
        : m_data(data)
        , m_index(0)
    { }

    uint8_t getByte()
    {
        const uint8_t val(reinterpret_cast<const uint8_t&>(m_data.at(m_index)));
        ++m_index;
        return val;
    }

    void getBytes(uint8_t* bytes, std::size_t length)
    {
        assert(m_index + length <= m_data.size());

        std::copy(
                m_data.data() + m_index,
                m_data.data() + m_index + length,
                bytes);

        m_index += length;
    }

private:
    const std::vector<char>& m_data;
    std::size_t m_index;
};

} // namespace entwine


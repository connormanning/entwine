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

#include <cstddef>
#include <cstdint>
#include <vector>

namespace entwine
{

class CompressionStream
{
public:
    CompressionStream();
    CompressionStream(const std::vector<char>& data);

    void putBytes(const uint8_t* bytes, std::size_t length);
    void putByte(uint8_t byte);

    uint8_t getByte();
    void getBytes(uint8_t* bytes, std::size_t length);

    const std::vector<char>& data() const;
    void clear() { m_data.clear(); m_index = 0; }

private:
    std::vector<char> m_data;
    std::size_t m_index;
};

} // namespace entwine


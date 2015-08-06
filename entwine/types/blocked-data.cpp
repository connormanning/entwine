/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/blocked-data.hpp>

namespace entwine
{

BlockedData::BlockedData(const std::size_t pointSize)
    : m_pointSize(pointSize)
    , m_index(0)
    , m_nextBlockPoints(16)
    , m_data()
{
    m_data.emplace_back(m_pointSize * 8);
}

void BlockedData::assign(const std::size_t n)
{
    if (m_index || m_data.size() > 1)
    {
        throw std::runtime_error("Cannot assign after modifying");
    }

    m_data.assign(1, std::vector<char>(m_pointSize * n));
}

char* BlockedData::getPointPos()
{
    auto& block(m_data.back());
    char* result(block.data() + m_index);

    m_index += m_pointSize;

    if (m_index == block.size())
    {
        m_data.emplace_back(m_pointSize * m_nextBlockPoints);
        m_index = 0;
        m_nextBlockPoints *= 2;
    }

    return result;
}

} // namespace entwine


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
#include <vector>

namespace entwine
{

class BlockedData
{
public:
    BlockedData(std::size_t pointSize);

    void assign(std::size_t numPoints);
    char* getPointPos();

private:
    std::size_t m_pointSize;

    std::size_t m_index;
    std::size_t m_nextBlockPoints;

    std::vector<std::vector<char>> m_data;
};

} // namespace entwine


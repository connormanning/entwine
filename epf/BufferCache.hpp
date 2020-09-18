#pragma once

#include <deque>

#include "EpfTypes.hpp"

namespace epf
{

class BufferCache
{
public:
    std::deque<DataVecPtr> m_buffers;
    std::mutex m_mutex;

    DataVecPtr fetch();
    void replace(DataVecPtr&& buf);
};

} // namespace epf

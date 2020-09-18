#include <mutex>

#include "BufferCache.hpp"

namespace epf
{

DataVecPtr BufferCache::fetch()
{
    std::lock_guard<std::mutex> lock(m_mutex);

    if (m_buffers.size())
    {
        DataVecPtr buf(std::move(m_buffers.back()));
        m_buffers.pop_back();
        return buf;
    }

    constexpr size_t BufSize = 4096 * 10;
    return DataVecPtr(new DataVec(BufSize));
}

void BufferCache::replace(DataVecPtr&& buf)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    m_buffers.push_back(std::move(buf));
}

} // namespace epf

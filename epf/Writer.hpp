#pragma once

#include <condition_variable>
#include <list>
#include <mutex>
#include <string>

#include <pdal/util/ThreadPool.hpp>

#include "EpfTypes.hpp"
#include "BufferCache.hpp"

namespace epf
{

class Writer
{
    struct WriteData
    {
        int index;
        std::unique_ptr<std::vector<uint8_t>> data;
    };

public:
    Writer(const std::string& directory, int numThreads);

    void enqueue(int index, DataVecPtr data);
    void stop();
    BufferCache& bufferCache()
        { return m_bufferCache; }

private:
    std::string path(int index);
    void run();

    std::string m_directory;
    pdal::ThreadPool m_pool;
    BufferCache m_bufferCache;
    bool m_stop;
    std::list<WriteData> m_queue;
    std::list<int> m_active;
    std::mutex m_mutex;
    std::condition_variable m_available;
};

} // namespace

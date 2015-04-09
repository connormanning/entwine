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

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace entwine
{

class Pool
{
public:
    Pool(std::size_t numThreads);
    ~Pool();

    // Add a threaded task, blocking until a thread is available.
    void add(std::function<void()> task);

    // Wait for all currently running tasks to complete.
    void join();

private:
    // Returns the index of the acquired thread.
    std::size_t acquire();

    // Release the thread at the specified index back into the pool.
    void release(std::size_t index);

    std::vector<std::thread> m_threads;
    std::vector<std::size_t> m_available;

    std::mutex m_mutex;
    std::condition_variable m_cv;
};

} // namespace entwine


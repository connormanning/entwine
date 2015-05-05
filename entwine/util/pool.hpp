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

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace entwine
{

class Pool
{
public:
    Pool(std::size_t numThreads);
    ~Pool();

    // Start worker threads
    void go();

    // Wait for all currently running tasks to complete.
    void join();

    // Add a threaded task, blocking until a thread is available.  If join() is
    // called, add() may not be called again until go() is called and completes.
    void add(std::function<void()> task);

private:
    // Worker thread function.  Wait for a task and run it - or if stop() is
    // called, complete any outstanding task and return.
    void work();

    // Atomically set/get the stop flag.
    bool stop();
    void stop(bool val);

    std::size_t m_numThreads;
    std::vector<std::thread> m_threads;
    std::queue<std::function<void()>> m_tasks;

    std::atomic<bool> m_stop;
    std::mutex m_mutex;
    std::condition_variable m_produceCv;
    std::condition_variable m_consumeCv;

    // Disable copy/assignment.
    Pool(const Pool& other);
    Pool& operator=(const Pool& other);
};

} // namespace entwine


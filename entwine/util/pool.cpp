/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/util/pool.hpp>

#include <cassert>
#include <iostream>

namespace entwine
{

namespace
{
    // Currently this isn't really much of a queue - after a task is enqueued
    // for which there is no current worker available, subsequent tasks will
    // block until it is consumed.
    const std::size_t queueSize(1);
}

Pool::Pool(const std::size_t numThreads)
    : m_numThreads(numThreads)
    , m_threads()
    , m_tasks()
    , m_stop(true)
    , m_mutex()
    , m_produceCv()
    , m_consumeCv()
{
    go();
}

Pool::~Pool()
{
    if (!stop()) join();
}

void Pool::go()
{
    if (!stop())
    {
        throw std::runtime_error(
                "Attempted to call Pool::go on an already running Pool");
    }

    stop(false);

    std::lock_guard<std::mutex> lock(m_mutex);

    for (std::size_t i(0); i < m_numThreads; ++i)
    {
        m_threads.emplace_back([this]() { work(); });
    }
}

void Pool::join()
{
    if (stop())
    {
        throw std::runtime_error(
                "Attempted to call Pool::join on an already joined Pool");
    }

    stop(true);
    m_consumeCv.notify_all();

    for (auto& t : m_threads)
    {
        t.join();
    }

    std::lock_guard<std::mutex> lock(m_mutex);
    m_threads.clear();
    assert(m_tasks.empty());
}

void Pool::add(std::function<void()> task)
{
    if (stop())
    {
        throw std::runtime_error(
                "Attempted to add a task to a non-running Pool");
    }

    std::unique_lock<std::mutex> lock(m_mutex);

    m_produceCv.wait(lock, [this]() { return m_tasks.size() < queueSize; });
    m_tasks.emplace(task);

    lock.unlock();

    // Notify worker that a task is available.
    m_consumeCv.notify_one();
}

void Pool::work()
{
    std::unique_lock<std::mutex> lock(m_mutex);

    while (!stop() || !m_tasks.empty())
    {
        m_consumeCv.wait(lock, [this]() { return !m_tasks.empty() || stop(); });

        if (!m_tasks.empty())
        {
            auto task(std::move(m_tasks.front()));
            m_tasks.pop();

            // Notify add(), which may be waiting for a spot in the queue.
            m_produceCv.notify_one();

            lock.unlock();

            try
            {
                task();
            }
            catch (std::runtime_error& e)
            {
                std::cout <<
                    "Exception caught in pool task: " << e.what() << std::endl;
            }
            catch (...)
            {
                std::cout <<
                    "Unknown exception caught in pool task." << std::endl;
            }

            lock.lock();
        }
    }
}

bool Pool::stop()
{
    return m_stop.load();
}

void Pool::stop(const bool val)
{
    m_stop.store(val);
}

} // namespace entwine


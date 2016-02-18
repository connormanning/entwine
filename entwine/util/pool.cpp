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
#include <limits>

namespace entwine
{

Pool::Pool(const std::size_t numThreads, const std::size_t queueSize)
    : m_numThreads(numThreads)
    , m_queueSize(queueSize)
    , m_threads()
    , m_tasks()
    , m_errors()
    , m_errorMutex()
    , m_stop(true)
    , m_deletions(0)
    , m_metaMutex()
    , m_workMutex()
    , m_produceCv()
    , m_consumeCv()
{
    go();
}

Pool::~Pool()
{
    join();
}

void Pool::go()
{
    std::lock_guard<std::mutex> lock(m_workMutex);

    if (!stop())
    {
        throw std::runtime_error(
                "Attempted to call Pool::go on an already running Pool");
    }

    stop(false);

    for (std::size_t i(0); i < m_numThreads; ++i)
    {
        m_threads.emplace_back([this]() { work(); });
    }
}

void Pool::join()
{
    if (!stop())
    {
        stop(true);

        m_consumeCv.notify_all();

        std::unique_lock<std::mutex> lock(m_workMutex);
        m_consumeCv.wait(lock, [this]() { return m_tasks.empty(); });
        lock.unlock();

        for (auto& t : m_threads)
        {
            m_consumeCv.notify_all();
            t.join();
        }

        m_threads.clear();
        assert(m_tasks.empty());
    }
}

bool Pool::joining() const
{
    return stop();
}

void Pool::add(std::function<void()> task)
{
    std::unique_lock<std::mutex> lock(m_workMutex);

    m_produceCv.wait(lock, [this]() { return m_tasks.size() < m_queueSize; });
    m_tasks.emplace(task);

    lock.unlock();

    // Notify worker that a task is available.
    m_consumeCv.notify_all();
}

void Pool::work()
{
    bool done(false);
    std::unique_lock<std::mutex> lock(m_workMutex);

    while (!done)
    {
        m_consumeCv.wait(lock, [this, &done]()
        {
            if (!done) done = (stop() && m_tasks.empty()) || shouldDelete();
            return done || !m_tasks.empty();
        });

        if (!m_tasks.empty())
        {
            auto task(std::move(m_tasks.front()));
            m_tasks.pop();

            lock.unlock();

            // Notify add(), which may be waiting for a spot in the queue.
            m_produceCv.notify_all();

            try
            {
                task();
            }
            catch (std::runtime_error& e)
            {
                std::cout <<
                    "Exception caught in pool task: " << e.what() << std::endl;

                std::lock_guard<std::mutex> lock(m_errorMutex);
                m_errors.push_back(e.what());
            }
            catch (...)
            {
                std::cout <<
                    "Unknown exception caught in pool task." << std::endl;

                std::lock_guard<std::mutex> lock(m_errorMutex);
                m_errors.push_back("Unknown error");
            }

            lock.lock();
        }

        if (!done) done = (stop() && m_tasks.empty()) || shouldDelete();
    }

    lock.unlock();

    m_consumeCv.notify_all();
    --m_numThreads;
}

void Pool::addWorker()
{
    std::lock_guard<std::mutex> lock(m_metaMutex);

    if (m_deletions)
    {
        --m_deletions;
    }
    else
    {
        ++m_numThreads;
        m_threads.emplace_back([this]() { work(); });
    }
}

void Pool::delWorker()
{
    std::lock_guard<std::mutex> lock(m_metaMutex);

    if (++m_deletions >= m_numThreads)
    {
        throw std::runtime_error("No threads left in the pool");
    }

    // Notify waiting tasks, so one of them may stop itself.
    m_consumeCv.notify_all();
}

bool Pool::stop() const
{
    std::lock_guard<std::mutex> lock(m_metaMutex);
    return m_stop;
}

void Pool::stop(const bool val)
{
    std::lock_guard<std::mutex> lock(m_metaMutex);
    m_stop = val;
}

bool Pool::shouldDelete()
{
    bool result(false);

    std::lock_guard<std::mutex> lock(m_metaMutex);
    if (m_deletions)
    {
        result = true;
        --m_deletions;
    }

    return result;
}

} // namespace entwine


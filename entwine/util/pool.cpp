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

namespace
{
    const std::size_t stopAll(std::numeric_limits<std::size_t>::max());
}

Pool::Pool(const std::size_t numThreads, const std::size_t queueSize)
    : m_numThreads(numThreads)
    , m_queueSize(queueSize)
    , m_threads()
    , m_tasks()
    , m_errors()
    , m_errorMutex()
    , m_stop(stopAll)
    , m_stopMutex()
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
        std::cout << "JOIN" << std::endl;
        stop(true);

        for (auto& t : m_threads)
        {
            m_consumeCv.notify_all();
            t.join();
        }

        std::lock_guard<std::mutex> lock(m_workMutex);
        m_threads.clear();
        assert(m_tasks.empty());
    }
}

bool Pool::joining() const
{
    return m_stop == stopAll;
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
    std::unique_lock<std::mutex> lock(m_workMutex);
    bool stopSelf(false);

    while (!stopSelf)
    {
        m_consumeCv.wait(lock, [this, &stopSelf]()
        {
            if (!stopSelf) stopSelf = stop();
            return stopSelf || !m_tasks.empty();
        });

        if (!stopSelf && !m_tasks.empty())
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

        if (!stopSelf)
        {
            stopSelf = stop();
        }
    }

    --m_numThreads;
    std::cout << "Removing a thread!" << std::endl;
}

void Pool::addWorker()
{
    std::lock_guard<std::mutex> lock(m_workMutex);

    ++m_numThreads;
    m_threads.emplace_back([this]() { work(); });
}

void Pool::delWorker()
{
    std::lock_guard<std::mutex> lock(m_stopMutex);
    if (m_stop != stopAll) ++m_stop;

    // Notify waiting tasks, if there are any, one of them will stop itself.
    m_consumeCv.notify_all();
}

bool Pool::stop()
{
    std::lock_guard<std::mutex> lock(m_stopMutex);
    bool result(m_stop);

    if (m_stop && m_stop != stopAll)
    {
        --m_stop;
    }

    return result;
}

void Pool::stop(const bool val)
{
    std::lock_guard<std::mutex> lock(m_stopMutex);

    if (val) m_stop = stopAll;
    else m_stop = 0;
}

} // namespace entwine


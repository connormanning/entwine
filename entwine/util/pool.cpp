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

#include <iostream>

namespace entwine
{

Pool::Pool(const std::size_t numThreads, const std::size_t queueSize)
    : m_numThreads(std::max<std::size_t>(numThreads, 1))
    , m_queueSize(std::max<std::size_t>(queueSize, 1))
{
    go();
}

Pool::~Pool()
{
    join();
}

void Pool::go()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_running) return;
    m_running = true;

    for (std::size_t i(0); i < m_numThreads; ++i)
    {
        m_threads.emplace_back([this]() { work(); });
    }
}

void Pool::join()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    if (!m_running) return;
    m_running = false;
    lock.unlock();

    m_consumeCv.notify_all();
    for (auto& t : m_threads) t.join();
    m_threads.clear();
}

void Pool::await()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_produceCv.wait(lock, [this]()
    {
        return !m_outstanding && m_tasks.empty();
    });
}

void Pool::add(std::function<void()> task)
{
    std::unique_lock<std::mutex> lock(m_mutex);

    if (!m_running)
    {
        throw std::runtime_error("Attempted to add a task to a stopped Pool");
    }

    m_produceCv.wait(lock, [this]() { return m_tasks.size() < m_queueSize; });
    m_tasks.emplace(task);

    // Notify worker that a task is available.
    lock.unlock();
    m_consumeCv.notify_all();
}

void Pool::work()
{
    while (true)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_consumeCv.wait(lock, [this]()
        {
            return m_tasks.size() || !m_running;
        });

        if (m_tasks.size())
        {
            ++m_outstanding;
            auto task(std::move(m_tasks.front()));
            m_tasks.pop();

            lock.unlock();

            // Notify add(), which may be waiting for a spot in the queue.
            m_produceCv.notify_all();

            std::string err;
            try { task(); }
            catch (std::exception& e) { err = e.what(); }
            catch (...) { err = "Unknown error"; }

            lock.lock();
            --m_outstanding;
            if (err.size())
            {
                std::cout << "Exception in pool task: " << err << std::endl;
                m_errors.push_back(err);
            }
            lock.unlock();

            // Notify await(), which may be waiting for a running task.
            m_produceCv.notify_all();
        }
        else if (!m_running)
        {
            return;
        }
    }
}

void Pool::resize(const std::size_t numThreads)
{
    join();
    m_numThreads = numThreads;
    go();
}

} // namespace entwine


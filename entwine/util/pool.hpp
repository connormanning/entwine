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
#include <deque>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace entwine
{

class Pool
{
public:
    // After numThreads tasks are actively running, and queueSize tasks have
    // been enqueued to wait for an available worker thread, subsequent calls
    // to Pool::add will block until an enqueued task has been popped from the
    // queue.
    Pool(
            std::size_t numThreads,
            std::size_t queueSize = 1,
            bool verbose = true)
        : m_verbose(verbose)
        , m_numThreads(std::max<std::size_t>(numThreads, 1))
        , m_queueSize(std::max<std::size_t>(queueSize, 1))
    {
        go();
    }

    ~Pool() { join(); }

    // Start worker threads.
    void go()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_running) return;
        m_running = true;

        for (std::size_t i(0); i < m_numThreads; ++i)
        {
            m_threads.emplace_back([this]() { work(); });
        }
    }

    // Disallow the addition of new tasks and wait for all currently running
    // tasks to complete.
    void join()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (!m_running) return;
        m_running = false;
        lock.unlock();

        m_consumeCv.notify_all();
        for (auto& t : m_threads) t.join();
        m_threads.clear();
    }

    // Wait for all current tasks to complete.  As opposed to join, tasks may
    // continue to be added while a thread is await()-ing the queue to empty.
    void await()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_produceCv.wait(lock, [this]()
        {
            return !m_outstanding && m_tasks.empty();
        });
    }

    // Join and restart.
    void cycle() { join(); go(); }

    // Change the number of threads.  Current threads will be joined.
    void resize(const std::size_t numThreads)
    {
        join();
        m_numThreads = numThreads;
        go();
    }

    // Not thread-safe, pool should be joined before calling.
    const std::vector<std::string>& errors() const { return m_errors; }

    // Add a threaded task, blocking until a thread is available.  If join() is
    // called, add() may not be called again until go() is called and completes.
    void add(std::function<void()> task)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (!m_running)
        {
            throw std::runtime_error(
                    "Attempted to add a task to a stopped Pool");
        }

        m_produceCv.wait(lock, [this]()
        {
            return m_tasks.size() < m_queueSize;
        });

        m_tasks.emplace_back(task);

        // Notify worker that a task is available.
        lock.unlock();
        m_consumeCv.notify_all();
    }

    // Add a threaded task, blocking until a thread is available.  If join() is
    // called, add() may not be called again until go() is called and completes.
    void addFront(std::function<void()> task)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (!m_running)
        {
            throw std::runtime_error(
                    "Attempted to add a task to a stopped Pool");
        }

        m_produceCv.wait(lock, [this]()
        {
            return m_tasks.size() < m_queueSize;
        });

        m_tasks.emplace_front(task);

        // Notify worker that a task is available.
        lock.unlock();
        m_consumeCv.notify_all();
    }


    std::size_t size() const { return m_numThreads; }
    std::size_t numThreads() const { return m_numThreads; }

private:
    // Worker thread function.  Wait for a task and run it - or if stop() is
    // called, complete any outstanding task and return.
    void work()
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
                m_tasks.pop_front();

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
                    if (m_verbose)
                    {
                        std::cout << "Exception in pool task: " << err <<
                            std::endl;
                    }
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

    bool m_verbose;
    std::size_t m_numThreads;
    std::size_t m_queueSize;
    std::vector<std::thread> m_threads;
    std::deque<std::function<void()>> m_tasks;

    std::vector<std::string> m_errors;
    std::mutex m_errorMutex;

    std::size_t m_outstanding = 0;
    bool m_running = false;

    mutable std::mutex m_mutex;
    std::condition_variable m_produceCv;
    std::condition_variable m_consumeCv;

    // Disable copy/assignment.
    Pool(const Pool& other);
    Pool& operator=(const Pool& other);
};

} // namespace entwine


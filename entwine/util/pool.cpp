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

#include <algorithm>
#include <iostream>

namespace entwine
{

Pool::Pool(const std::size_t numThreads)
    : m_threads(numThreads)
    , m_available(numThreads)
    , m_mutex()
    , m_cv()
{
    std::size_t i(0);
    std::generate(m_available.begin(), m_available.end(), [&i]{ return i++; });
}

Pool::~Pool()
{
    join();
}

void Pool::add(std::function<void()> task)
{
    const std::size_t index(acquire());

    m_threads[index] = std::thread([this, index, task]()
    {
        try
        {
            task();
        }
        catch (std::runtime_error& e)
        {
            std::cout << "Caught: " << e.what() << std::endl;
        }
        catch (...)
        {
            std::cout << "Exception caught in Pool task." << std::endl;
        }

        release(index);
    });
}

void Pool::join()
{
    std::unique_lock<std::mutex> lock(m_mutex);

    m_cv.wait(lock, [this]()->bool
    {
        return m_available.size() == m_threads.size();
    });
}

std::size_t Pool::acquire()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait(lock, [this]()->bool { return m_available.size(); });

    const std::size_t index(m_available.back());
    m_available.pop_back();

    lock.unlock();

    return index;
}

void Pool::release(const std::size_t index)
{
    std::unique_lock<std::mutex> lock(m_mutex);

    m_available.push_back(index);
    m_threads[index].detach();
    lock.unlock();

    m_cv.notify_all();
}

} // namespace entwine


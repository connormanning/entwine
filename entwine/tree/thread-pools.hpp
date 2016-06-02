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

#include <entwine/util/pool.hpp>

namespace entwine
{

class ThreadPools
{
public:
    ThreadPools(std::size_t totalThreads);

    Pool& workPool() { return m_workPool; }
    Pool& clipPool() { return m_clipPool; }

    const Pool& workPool() const { return m_workPool; }
    const Pool& clipPool() const { return m_clipPool; }

    std::size_t size() const
    {
        return m_workPool.numThreads() + m_clipPool.numThreads();
    }

    void join()
    {
        m_workPool.join();
        m_clipPool.join();
    }

private:
    Pool m_workPool;
    Pool m_clipPool;
};

} // namespace entwine


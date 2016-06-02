/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <cmath>

#include <entwine/tree/thread-pools.hpp>

namespace
{
    const double workToClipRatio(0.33);

    std::size_t getWorkThreads(const std::size_t total)
    {
        std::size_t num(
                std::llround(static_cast<double>(total) * workToClipRatio));
        return std::max<std::size_t>(num, 1);
    }

    std::size_t getClipThreads(const std::size_t total)
    {
        return std::max<std::size_t>(total - getWorkThreads(total), 4);
    }
}

namespace entwine
{

ThreadPools::ThreadPools(const std::size_t totalThreads)
    : m_workPool(getWorkThreads(totalThreads))
    , m_clipPool(getClipThreads(totalThreads))
{ }

} //namespace entwine


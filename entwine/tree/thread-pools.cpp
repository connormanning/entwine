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
    const double defaultWorkToClipRatio(0.33);

    std::size_t getWorkThreads(
            const std::size_t total,
            double workToClipRatio = defaultWorkToClipRatio)
    {
        std::size_t num(
                std::llround(static_cast<double>(total) * workToClipRatio));
        return std::max<std::size_t>(num, 1);
    }

    std::size_t getClipThreads(
            const std::size_t total,
            double workToClipRatio = defaultWorkToClipRatio)
    {
        return std::max<std::size_t>(
                total - getWorkThreads(total, workToClipRatio),
                4);
    }
}

namespace entwine
{

ThreadPools::ThreadPools(const std::size_t totalThreads)
    : m_workPool(getWorkThreads(totalThreads))
    , m_clipPool(getClipThreads(totalThreads))
    , m_ratio(defaultWorkToClipRatio)
{ }

void ThreadPools::setRatio(const double r)
{
    const std::size_t total(size());
    m_workPool.resize(getWorkThreads(total, r));
    m_clipPool.resize(getClipThreads(total, r));
}

} //namespace entwine


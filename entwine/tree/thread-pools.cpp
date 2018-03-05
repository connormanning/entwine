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

namespace entwine
{

ThreadPools::ThreadPools(const std::size_t t)
    : ThreadPools(getWorkThreads(t), getClipThreads(t))
{ }

ThreadPools::ThreadPools(
        const std::size_t workThreads,
        const std::size_t clipThreads)
    : m_workPool(std::max<std::size_t>(1, workThreads))
    , m_clipPool(std::max<std::size_t>(4, clipThreads))
{ }

std::size_t ThreadPools::getWorkThreads(
        const std::size_t total,
        const double workToClipRatio)
{
    std::size_t num(
            std::llround(static_cast<double>(total) * workToClipRatio));
    return std::max<std::size_t>(num, 1);
}

std::size_t ThreadPools::getClipThreads(
        const std::size_t total,
        const double workToClipRatio)
{
    return std::max<std::size_t>(
            total - getWorkThreads(total, workToClipRatio),
            4);
}

} //namespace entwine


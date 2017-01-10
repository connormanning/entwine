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

#include <entwine/tree/heuristics.hpp>

namespace entwine
{

namespace
{
    std::size_t getWorkThreads(
            const std::size_t total,
            double workToClipRatio = heuristics::defaultWorkToClipRatio)
    {
        std::size_t num(
                std::llround(static_cast<double>(total) * workToClipRatio));
        return std::max<std::size_t>(num, 1);
    }

    std::size_t getClipThreads(
            const std::size_t total,
            double workToClipRatio = heuristics::defaultWorkToClipRatio)
    {
        return std::max<std::size_t>(
                total - getWorkThreads(total, workToClipRatio),
                4);
    }
}

ThreadPools::ThreadPools(const std::size_t totalThreads)
    : m_workPool(getWorkThreads(totalThreads))
    , m_clipPool(getClipThreads(totalThreads))
    , m_ratio(heuristics::defaultWorkToClipRatio)
{ }

} //namespace entwine


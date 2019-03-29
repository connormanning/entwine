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

#include <chrono>
#include <fstream>
#include <iostream>
#include <string>

namespace entwine
{

using Clock = std::chrono::high_resolution_clock;
using TimePoint = Clock::time_point;

inline TimePoint now()
{
    return Clock::now();
}

template<typename T>
inline int since(TimePoint start)
{
    const std::chrono::duration<double> d(now() - start);
    return std::chrono::duration_cast<T>(d).count();
}

} // namespace entwine


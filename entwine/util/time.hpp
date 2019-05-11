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

// Format a duration in seconds to HH:MM:SS.
inline std::string formatTime(const int seconds)
{
    const int h(seconds / 60 / 60);
    const int m((seconds / 60) % 60);
    const int s(seconds % 60);

    auto pad([](int v)->std::string
    {
        return (v < 10 ? "0" : "") + std::to_string(v);
    });

    return (h ? pad(h) + ":" : "") + pad(m) + ":" + pad(s);
}

} // namespace entwine


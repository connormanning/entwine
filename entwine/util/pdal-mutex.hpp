/******************************************************************************
* Copyright (c) 2020, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <mutex>

namespace entwine
{

class PdalMutex
{
public:
    static std::mutex& get()
    {
        static PdalMutex instance;
        return instance.mutex;
    }

private:
    PdalMutex() = default;
    std::mutex mutex;
};

} // namespace entwine

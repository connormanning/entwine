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

#include <memory>
#include <mutex>

namespace entwine
{

class Executor
{
public:
    static Executor& get()
    {
        static Executor e;
        return e;
    }

    static std::mutex& mutex()
    {
        return get().m_mutex;
    }

    static std::unique_lock<std::mutex> getLock()
    {
        return std::unique_lock<std::mutex>(mutex());
    }

    mutable std::mutex m_mutex;
};

} // namespace entwine


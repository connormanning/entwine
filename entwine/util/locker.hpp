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

#include <atomic>

namespace entwine
{

class Locker
{
public:
    explicit Locker(std::atomic_flag& flag)
        : m_flag(flag)
    {
        while (m_flag.test_and_set())
            ;
    }

    ~Locker() { m_flag.clear(); }

private:
    std::atomic_flag& m_flag;
};

} // namespace entwine


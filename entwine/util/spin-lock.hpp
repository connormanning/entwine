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

#ifdef SPINLOCK_AS_MUTEX
#include <mutex>
#else
#include <atomic>
#endif

namespace entwine
{

#ifdef SPINLOCK_AS_MUTEX

using SpinLock = std::mutex;

#else

class SpinLock
{
public:
    SpinLock() = default;

    void lock() { while (m_flag.test_and_set()) ; }
    void unlock() { m_flag.clear(); }

private:
    std::atomic_flag m_flag = ATOMIC_FLAG_INIT;

    SpinLock(const SpinLock& other) = delete;
};

#endif

using SpinGuard = std::lock_guard<SpinLock>;
using UniqueSpin = std::unique_lock<SpinLock>;

} // namespace entwine


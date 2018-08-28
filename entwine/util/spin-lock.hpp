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

class SpinLock
{
    friend class SpinGuard;
    friend class UniqueSpin;

public:
    SpinLock() = default;

private:
    std::mutex m_mutex;
    std::mutex& mutex() { return m_mutex; }

    SpinLock(const SpinLock& other) = delete;
};

class SpinGuard
{
public:
    SpinGuard(SpinLock& m) : m_lock(m.mutex()) { }

private:
    std::lock_guard<std::mutex> m_lock;
};

class UniqueSpin
{
public:
    UniqueSpin(SpinLock& m) : m_lock(m.mutex()) { }

    void lock() { m_lock.lock(); }
    void unlock() { m_lock.unlock(); }

private:
    std::unique_lock<std::mutex> m_lock;
};

#else

class SpinLock
{
    friend class SpinGuard;
    friend class UniqueSpin;

public:
    SpinLock() = default;

private:
    void lock() { while (m_flag.test_and_set()) ; }
    void unlock() { m_flag.clear(); }

    std::atomic_flag m_flag = ATOMIC_FLAG_INIT;

    SpinLock(const SpinLock& other) = delete;
};

class SpinGuard
{
public:
    SpinGuard(SpinLock& m) : m_spinner(m) { m_spinner.lock(); }
    ~SpinGuard() { m_spinner.unlock(); }

private:
    SpinLock& m_spinner;
};

class UniqueSpin
{
public:
    UniqueSpin(SpinLock& m)
        : m_spinner(m)
        , m_locked(true)
    {
        m_spinner.lock();
    }

    ~UniqueSpin() { if (m_locked) m_spinner.unlock(); }

    void lock() { m_spinner.lock(); m_locked = true; }  // UB if locked.
    void unlock() { m_spinner.unlock(); m_locked = false; }

private:
    SpinLock& m_spinner;
    bool m_locked;
};

#endif

} // namespace entwine


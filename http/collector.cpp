/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "collector.hpp"

Collector::Collector(const std::size_t numExpected)
    : m_expected(numExpected)
    , m_received(0)
    , m_shouldSlowDown(false)
    , m_waitList()
    , m_mutex()
    , m_cv()
{ }

Collector::~Collector()
{ }

void Collector::waitFor(std::size_t num)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_waitList.insert(num);
    num = std::min(num, m_expected);
    m_cv.wait(lock, [this, num]()->bool {
        return m_received >= num;
    });
}

bool Collector::shouldSlowDown() const
{
    return m_shouldSlowDown;
}

void Collector::shouldSlowDown(bool val)
{
    m_shouldSlowDown = val;
}

void Collector::inc()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    if (++m_received == m_expected || m_waitList.count(m_received)) {
        while (
                !m_waitList.empty() &&
                *(m_waitList.begin()) <= m_received)
        {
            m_waitList.erase(m_waitList.begin());
        }

        lock.unlock();
        m_cv.notify_all();
    }
}

///////////////////////////////////////////////////////////////////////////////

PutCollector::PutCollector(const std::size_t numExpected)
    : Collector(numExpected)
    , m_errs()
{ }

PutCollector::~PutCollector()
{ }

void PutCollector::insert(
    uint64_t id,
    HttpResponse res,
    const std::shared_ptr<std::vector<uint8_t>> data)
{
    if (res.code() != 200)
    {
        if (res.code() / 100 == 5) m_shouldSlowDown = true;

        std::lock_guard<std::mutex> lock(m_mutex);
        m_errs.insert(std::make_pair(id, data));
    }

    // Increment at the end, so if this is the last response, we will
    // notify 'done' after all possible errors have been captured.
    inc();
}

std::map<uint64_t, const std::shared_ptr<std::vector<uint8_t>>>
PutCollector::errs()
{
    waitFor(m_expected);
    std::map<uint64_t, const std::shared_ptr<std::vector<uint8_t>>> tmp;
    m_errs.swap(tmp);
    return tmp;
}

///////////////////////////////////////////////////////////////////////////////

GetCollector::GetCollector(const std::size_t numExpected)
    : Collector(numExpected)
    , m_data()
    , m_errs()
{ }

GetCollector::~GetCollector()
{ }

void GetCollector::insert(uint64_t id, std::string file, HttpResponse res)
{
    if (res.code() == 200)
    {
        m_data.insert(std::make_pair(id, res.data()));
    }
    else
    {
        if (res.code() != 404)
        {
            // 404s are expected, since we are querying nodes from a theoretical
            // completely full tree.

            if (res.code() / 100 == 5) m_shouldSlowDown = true;

            std::lock_guard<std::mutex> lock(m_mutex);
            m_errs.insert(std::make_pair(id, file));
        }
    }

    // Increment at the end, so if this is the last response, we will
    // notify 'done' after all possible errors have been captured.
    inc();
}

std::map<uint64_t, std::string> GetCollector::errs()
{
    waitFor(m_expected);
    std::map<uint64_t, std::string> tmp;
    m_errs.swap(tmp);
    return tmp;
}

std::map<uint64_t, const std::shared_ptr<std::vector<uint8_t>>>
GetCollector::responses()
{
    return m_data;
}


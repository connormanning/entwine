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

#include <condition_variable>
#include <mutex>
#include <thread>

#include <entwine/http/s3.hpp>
#include <entwine/tree/point-info.hpp>

namespace entwine
{

class SleepyTree;

class MultiBatcher
{
public:
    MultiBatcher(
            const S3Info& s3Info,
            std::size_t numBatches,
            SleepyTree& sleepyTree);
    ~MultiBatcher();

    void add(const std::string& filename, Origin origin);

    // Await all outstanding responses.
    void gather();

private:
    S3 m_s3;
    std::vector<std::thread> m_batches;
    std::vector<std::size_t> m_available;
    SleepyTree& m_sleepyTree;

    std::mutex m_mutex;
    std::condition_variable m_cv;
};

} // namespace entwine


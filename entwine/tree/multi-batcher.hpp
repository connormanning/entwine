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
            SleepyTree& sleepyTree,
            std::size_t numThreads,
            std::size_t pointBatchSize = 0,
            std::size_t snapshot = 0);
    ~MultiBatcher();

    // Add a pointcloud file to this index.
    void add(const std::string& filename);

    // Wait for all batched additions to complete.
    void gather();

private:
    void takeSnapshot();

    S3 m_s3;
    std::vector<std::thread> m_threads;
    std::vector<std::size_t> m_available;
    std::vector<std::string> m_originList;

    SleepyTree& m_sleepyTree;

    const std::size_t m_pointBatchSize;
    const std::size_t m_snapshot;
    bool m_allowAdd;

    std::mutex m_mutex;
    std::condition_variable m_cv;
};

} // namespace entwine


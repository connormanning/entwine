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

#include "http/s3.hpp"
#include "tree/sleepy-tree.hpp"

class MultiBatcher
{
public:
    MultiBatcher(
            const S3Info& s3Info,
            const std::string& outPath,
            std::size_t numBatches,
            std::shared_ptr<SleepyTree> sleepyTree);

    void add(const std::string& filename, Origin origin);

    // Must be called before destruction.
    void gather();

private:
    S3 m_s3;
    const std::string m_outPath;
    std::vector<std::thread> m_batches;
    std::vector<std::size_t> m_available;
    std::shared_ptr<SleepyTree> m_sleepyTree;

    std::mutex m_mutex;
    std::condition_variable m_cv;
};


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
#include <cstddef>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <pdal/Dimension.hpp>

#include <entwine/builder/config.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/file-info.hpp>
#include <entwine/types/outer-scope.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/util/time.hpp>

namespace Json
{
    class Value;
}

namespace entwine
{

namespace arbiter
{
    class Arbiter;
    class Endpoint;
}

class Bounds;
class Clipper;
class Executor;
class FileInfo;
class Metadata;
class Pool;
class Registry;
class Reprojection;
class Schema;
class Sequence;
class Structure;
class Subset;
class ThreadPools;

class Builder
{
    friend class Clipper;
    friend class Merger;
    friend class Sequence;

public:
    Builder(const Config& config, OuterScope os = OuterScope());
    ~Builder();

    // Perform indexing.  A _maxFileInsertions_ of zero inserts all files.
    void go(std::size_t maxFileInsertions = 0);

    // Aggregate spatially segmented build.
    void merge(Builder& other, Clipper& clipper);

    // Various getters.
    const Metadata& metadata() const;
    const Registry& registry() const;
    ThreadPools& threadPools() const;
    arbiter::Arbiter& arbiter();
    const arbiter::Arbiter& arbiter() const;
    const Sequence& sequence() const;
    Sequence& sequence();

    PointPool& pointPool() const;
    std::shared_ptr<PointPool> sharedPointPool() const;

    bool isContinuation() const { return m_isContinuation; }
    std::size_t sleepCount() const { return m_sleepCount; }

    const arbiter::Endpoint& outEndpoint() const;
    const arbiter::Endpoint& tmpEndpoint() const;

    // Set up our metadata as finished with merging.
    void makeWhole();

    void append(const FileInfoList& fileInfo);

    bool verbose() const { return m_verbose; }
    void verbose(bool v) { m_verbose = v; }
    uint64_t resetFiles() const { return m_resetFiles; }

    const Config& inConfig() const { return m_config; }

private:
    Registry& registry();
    void doRun(std::size_t max);

    std::mutex& mutex();

    // Save the current state of the tree.  Files may no longer be inserted
    // after this call, but getters are still valid.
    void save();
    void save(std::string to);
    void save(const arbiter::Endpoint& to);

    void cycle();

    // Insert points from a file.  Sets any previously unset FileInfo fields
    // based on file contents.
    void insertPath(Origin origin, FileInfo& info);

    // Returns a stack of rejected info nodes so that they may be reused.
    // Cells insertData(Cells cells, Clipper& clipper);

    // Validate sources.
    void prepareEndpoints();

    // Ensure that the file at this path is accessible locally for execution.
    // Return the local path.
    std::string localize(std::string path, Origin origin);

    //

    const Config m_config;

    std::shared_ptr<arbiter::Arbiter> m_arbiter;
    std::unique_ptr<arbiter::Endpoint> m_out;
    std::unique_ptr<arbiter::Endpoint> m_tmp;

    std::unique_ptr<ThreadPools> m_threadPools;

    const bool m_isContinuation = false;
    const std::size_t m_sleepCount;
    std::unique_ptr<Metadata> m_metadata;

    mutable std::mutex m_mutex;
    mutable std::shared_ptr<PointPool> m_pointPool;

    std::unique_ptr<Registry> m_registry;
    std::unique_ptr<Sequence> m_sequence;

    bool m_verbose;

    TimePoint m_start;
    TimePoint m_reset;
    const int m_resetMinutes = 60;
    const uint64_t m_resetFiles = 0;

    Builder(const Builder&);
    Builder& operator=(const Builder&);
};

} // namespace entwine


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

#include <entwine/tree/new-climber.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/manifest.hpp>
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
class NewClipper;
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
    Builder(const Config& config);

    // Launch a new build.
    Builder(
            const Metadata& metadata,
            std::string outPath,
            std::string tmpPath,
            std::size_t workThreads,
            std::size_t clipThreads,
            OuterScope outerScope = OuterScope());

    /*
    // Continue an existing build.
    Builder(
            std::string outPath,
            std::string tmpPath,
            std::size_t workThreads,
            std::size_t clipThreads,
            const std::size_t* subsetId = nullptr,
            OuterScope outerScope = OuterScope());
    */

    ~Builder();

    // Perform indexing.  A _maxFileInsertions_ of zero inserts all files in
    // the manifest.
    void go(std::size_t maxFileInsertions = 0);

    // Aggregate spatially segmented build.
    void merge(Builder& other);

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

    const arbiter::Endpoint& outEndpoint() const;
    const arbiter::Endpoint& tmpEndpoint() const;

    // Set up our metadata as finished with merging.
    void makeWhole();
    void unbump();

    void append(const FileInfoList& fileInfo);

    bool verbose() const { return m_verbose; }
    void verbose(bool v) { m_verbose = v; }

    static std::unique_ptr<Builder> tryCreateExisting(
            std::string path,
            std::string tmp,
            std::size_t workThreads,
            std::size_t clipThreads,
            const std::size_t* subsetId = nullptr,
            OuterScope outerScope = OuterScope());

private:
    void doRun(std::size_t max);
    bool exists() const { return !!m_metadata->manifestPtr(); }

    std::mutex& mutex();

    // Save the current state of the tree.  Files may no longer be inserted
    // after this call, but getters are still valid.
    void save();
    void save(std::string to);
    void save(const arbiter::Endpoint& to);

    // Insert points from a file.  Sets any previously unset FileInfo fields
    // based on file contents.
    void insertPath(Origin origin, FileInfo& info);

    // Returns a stack of rejected info nodes so that they may be reused.
    Cells insertData(
            Cells cells,
            Origin origin,
            NewClipper& clipper,
            NewClimber& climber);

    // Remove resources that are no longer needed.
    void clip(
            const Id& index,
            std::size_t chunkNum,
            std::size_t id,
            bool sync = false);

    void clip(uint64_t d, uint64_t x, uint64_t y);

    // Validate sources.
    void prepareEndpoints();

    // Ensure that the file at this path is accessible locally for execution.
    // Return the local path.
    std::string localize(std::string path, Origin origin);

    //

    std::shared_ptr<arbiter::Arbiter> m_arbiter;
    std::unique_ptr<arbiter::Endpoint> m_out;
    std::unique_ptr<arbiter::Endpoint> m_tmp;

    std::unique_ptr<ThreadPools> m_threadPools;
    std::unique_ptr<Metadata> m_metadata;

    mutable std::mutex m_mutex;
    bool m_isContinuation;

    mutable std::shared_ptr<PointPool> m_pointPool;

    std::unique_ptr<Sequence> m_sequence;
    std::unique_ptr<Registry> m_registry;

    bool m_verbose = false;

    TimePoint m_start;

    Builder(const Builder&);
    Builder& operator=(const Builder&);
};

} // namespace entwine


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

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <pdal/Dimension.hpp>

#include <entwine/tree/climber.hpp>
#include <entwine/tree/hierarchy.hpp>
#include <entwine/tree/manifest.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/outer-scope.hpp>
#include <entwine/types/point-pool.hpp>

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
    friend class Reader;
    friend class Sequence;

public:
    // Launch a new build.
    Builder(
            const Metadata& metadata,
            std::string outPath,
            std::string tmpPath,
            std::size_t numThreads,
            OuterScope outerScope = OuterScope());

    // Continue an existing build.
    Builder(
            std::string outPath,
            std::string tmpPath,
            std::size_t numThreads,
            const std::size_t* subsetId = nullptr,
            OuterScope outerScope = OuterScope());

    ~Builder();

    // Perform indexing.  A _maxFileInsertions_ of zero inserts all files in
    // the manifest.
    void go(std::size_t maxFileInsertions = 0);

    // Save the current state of the tree.  Files may no longer be inserted
    // after this call, but getters are still valid.
    void save();
    void save(std::string to);
    void save(const arbiter::Endpoint& to);

    // Aggregate spatially segmented build.
    void merge(Builder& other);

    // Various getters.
    const Metadata& metadata() const;
    const Registry& registry() const;
    const Hierarchy& hierarchy() const;
    ThreadPools& threadPools() const;
    const arbiter::Arbiter& arbiter() const;
    const Sequence& sequence() const;
    Sequence& sequence();

    PointPool& pointPool() const;
    std::shared_ptr<PointPool> sharedPointPool() const;
    std::shared_ptr<HierarchyCell::Pool> sharedHierarchyPool() const;

    bool isContinuation() const { return m_isContinuation; }

    const arbiter::Endpoint& outEndpoint() const;
    const arbiter::Endpoint& tmpEndpoint() const;

    // Set up our metadata as finished with merging.
    void makeWhole();

    void append(const Manifest& manifest);

    bool verbose() const { return m_verbose; }
    void verbose(bool v) { m_verbose = v; }

private:
    Executor& executor();
    std::mutex& mutex();

    // Attempt to wake up a possibly subset build with indeterminate metadata
    // state.  Used to wake up the active Builder for merging.
    static std::unique_ptr<Builder> create(
            std::string path,
            std::size_t threads,
            const std::size_t* subsetId,
            OuterScope outerScope = OuterScope());

    // Insert points from a file.  Return true if successful.  Sets any
    // previously unset FileInfo fields based on file contents.
    bool insertPath(Origin origin, FileInfo& info);

    // Returns a stack of rejected info nodes so that they may be reused.
    Cell::PooledStack insertData(
            Cell::PooledStack cells,
            Origin origin,
            Clipper& clipper,
            Climber& climber);

    // Remove resources that are no longer needed.
    void clip(
            const Id& index,
            std::size_t chunkNum,
            std::size_t id,
            bool sync = false);

    // Validate sources.
    void prepareEndpoints();

    // Ensure that the file at this path is accessible locally for execution.
    // Return the local path.
    std::string localize(std::string path, Origin origin);

    void addError(const std::string& path, const std::string& error);

    //

    std::shared_ptr<arbiter::Arbiter> m_arbiter;
    std::unique_ptr<arbiter::Endpoint> m_outEndpoint;
    std::unique_ptr<arbiter::Endpoint> m_tmpEndpoint;

    std::unique_ptr<Metadata> m_metadata;

    mutable std::mutex m_mutex;
    bool m_isContinuation;

    std::unique_ptr<ThreadPools> m_threadPools;
    std::unique_ptr<Executor> m_executor;
    std::unique_ptr<Sequence> m_sequence;

    pdal::Dimension::Id m_originId;

    mutable std::shared_ptr<PointPool> m_pointPool;
    mutable std::shared_ptr<HierarchyCell::Pool> m_hierarchyPool;

    std::unique_ptr<Hierarchy> m_hierarchy;
    std::unique_ptr<Registry> m_registry;

    bool m_verbose = false;

    Builder(const Builder&);
    Builder& operator=(const Builder&);
};

} // namespace entwine


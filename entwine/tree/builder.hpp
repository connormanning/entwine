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
#include <entwine/tree/manifest.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/outer-scope.hpp>
#include <entwine/types/point-pool.hpp>

namespace Json
{
    class Value;
}

namespace arbiter
{
    class Arbiter;
    class Endpoint;
}

namespace entwine
{

class BBox;
class Clipper;
class Executor;
class FileInfo;
class Metadata;
class Pool;
class Registry;
class Reprojection;
class Schema;
class Structure;
class Subset;
class ThreadPools;

class Builder
{
    friend class Clipper;
    friend class Merger;
    friend class Reader;

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

    // Aggregate manifest-split build.
    void unsplit(Builder& other);

    // Aggregate spatially segmented build.
    void merge(Builder& other);

    // Various getters.
    const Metadata& metadata() const;
    const Registry& registry() const;
    const Hierarchy& hierarchy() const;
    ThreadPools& threadPools() const;
    const arbiter::Arbiter& arbiter() const;

    PointPool& pointPool() const;
    std::shared_ptr<PointPool> sharedPointPool() const;
    Node::NodePool& nodePool() const;
    std::shared_ptr<Node::NodePool> sharedNodePool() const;

    bool isContinuation() const { return m_isContinuation; }

    const arbiter::Endpoint& outEndpoint() const;
    const arbiter::Endpoint& tmpEndpoint() const;

    // Stop this build as soon as possible.  All partially inserted paths will
    // be completed, and non-inserted paths can be added by continuing this
    // build later.
    void stop();

    // Set up our metadata as finished with merging.
    void makeWhole();

    // Mark about half of the remaining work on the current build as "not our
    // problem" - the remaining work can be done separately and we can merge
    // it later.  If successfully split, the result is a Manifest::Split
    // containing the manifest indices that should be built elsewhere.  If the
    // result points to nullptr, then this builder has refused to give up any
    // work and will complete the entirety of the build.
    std::unique_ptr<Manifest::Split> takeWork();

    // Read-only.  Used by the Reader to avoid duplicating metadata logic (if
    // no subset/split is passed) or by the Merger to awaken partial builds.
    //
    // Also used for merging, after an initial Builder::create has provided
    // us with enough metadata info to fetch the other pieces directly.
    //
    // Also used for traversing.
    /*
    Builder(
            std::string path,
            std::size_t threads = 1,
            const std::size_t* subsetId = nullptr,
            const std::size_t* splitBegin = nullptr,
            OuterScope outerScope = OuterScope());
            */

private:
    // Attempt to wake up a subset or split build with indeterminate metadata
    // state.  Used to wake up the active Builder for merging.
    static std::unique_ptr<Builder> create(
            std::string path,
            std::size_t threads,
            OuterScope outerScope = OuterScope());

    // Used to wake up subsequent Builders for merging.
    static std::unique_ptr<Builder> create(
            std::string path,
            std::size_t threads,
            std::size_t subsetId,
            OuterScope outerScope = OuterScope());

    // Returns true if we should insert this file based on its info.
    bool checkInfo(const FileInfo& info);

    // Returns true if we should insert this file based on its bounds.
    bool checkBounds(Origin origin, const BBox& bbox, std::size_t numPoints);

    // Insert points from a file.  Return true if successful.  Sets any
    // previously unset FileInfo fields based on file contents.
    bool insertPath(Origin origin, FileInfo& info);

    // Returns a stack of rejected info nodes so that they may be reused.
    Cell::PooledStack insertData(
            Cell::PooledStack cells,
            Origin origin,
            Clipper& clipper,
            Climber& climber);

    typedef std::map<Id, std::vector<CellState>> Reserves;

    // Insert within a previously-identified depth range.
    void insertHinted(
            Reserves& reserves,
            Cell::PooledStack cells,
            PointStatsMap& pointStatsMap,
            Clipper& clipper,
            Hierarchy& localHierarchy,
            const Id& chunkId,
            std::size_t depthBegin,
            std::size_t depthEnd = 0);

    // Remove resources that are no longer needed.
    void clip(const Id& index, std::size_t chunkNum, std::size_t id);

    // Awaken the tree from a saved state.
    void load(
            OuterScope outerScope,
            std::size_t clipThreads,
            std::string postfix = "");

    void load(
            OuterScope outerScope,
            const std::size_t* subsetId,
            const std::size_t* splitBegin);

    // Validate sources.
    void prepareEndpoints();

    // Initialize the SRS from the preview for this path.
    void initSrs(const std::string& path);

    // Callers of these functions must not hold a lock on m_mutex.
    Origin end() const;
    bool keepGoing() const;
    void next();

    // Ensure that the file at this path is accessible locally for execution.
    // Return the local path.
    std::string localize(std::string path, Origin origin);

    // Get metadata properties, and load from those serialized properties.
    void loadProps(OuterScope outerScope, Json::Value& props, std::string pf);

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

    pdal::Dimension::Id::Enum m_originId;
    Origin m_origin;
    Origin m_end;
    std::size_t m_added;

    mutable std::shared_ptr<PointPool> m_pointPool;
    mutable std::shared_ptr<Node::NodePool> m_nodePool;

    std::unique_ptr<Hierarchy> m_hierarchy;
    std::unique_ptr<Registry> m_registry;

    Builder(const Builder&);
    Builder& operator=(const Builder&);
};

} // namespace entwine


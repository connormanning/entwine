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

#include <entwine/tree/manifest.hpp>
#include <entwine/tree/point-info.hpp>

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
class Driver;
class Executor;
class Manifest;
class Pool;
class Pools;
class Registry;
class Reprojection;
class Schema;
class Structure;
class Subset;

class Builder
{
    friend class Clipper;

public:
    // Launch a new build.
    Builder(
            std::unique_ptr<Manifest> manifest,
            std::string outPath,
            std::string tmpPath,
            bool compress,
            bool trustHeaders,
            const Subset* subset,
            const Reprojection* reprojection,
            const BBox& bbox,
            const Schema& schema,
            std::size_t numThreads,
            const Structure& structure,
            std::shared_ptr<arbiter::Arbiter> arbiter = 0);

    // Continue an existing build.
    Builder(
            std::string outPath,
            std::string tmpPath,
            std::size_t numThreads,
            std::shared_ptr<arbiter::Arbiter> arbiter = 0);

    // Doesn't insert points, but metadata can be altered or different
    // builders may be merged.
    Builder(
            std::string path,
            std::size_t subsetId = 0,   // One-based.  Zero means no subset.
            std::shared_ptr<arbiter::Arbiter> arbiter = 0);

    ~Builder();

    // Perform indexing.  A _maxFileInsertions_ of zero inserts all files in
    // the manifest.
    void go(std::size_t maxFileInsertions = 0);

    // Save the current state of the tree.  Files may no longer be inserted
    // after this call, but getters are still valid.
    void save();

    // Aggregate segmented build.
    void merge();

    // Various getters.
    const BBox& bbox() const;
    const Schema& schema() const;
    const Manifest& manifest() const;
    const Structure& structure() const;
    const Registry& registry() const;
    const Subset* subset() const;
    const Reprojection* reprojection() const;
    Pools& pools() const;

    bool compress() const       { return m_compress; }
    bool trustHeaders() const   { return m_trustHeaders; }
    bool isContinuation() const { return m_isContinuation; }
    bool chunkExists(const Id& id) const;

    const std::string& srs() const { return m_srs; }
    std::size_t numThreads() const;

    const arbiter::Endpoint& outEndpoint() const;
    const arbiter::Endpoint& tmpEndpoint() const;

    // Stop this build as soon as possible.  All partially inserted paths will
    // be completed, and non-inserted paths can be added by continuing this
    // build later.
    void stop();

    // Mark about half of the remaining work on the current build as "not our
    // problem" - the remaining work can be done separately and we can merge
    // it later.  If successfully split, the result is a Manifest::Split
    // containing the manifest indices that should be built elsewhere.  If the
    // result points to nullptr, then this builder has refused to give up any
    // work and will complete the entirety of the build.
    std::unique_ptr<Manifest::Split> takeWork();

private:
    // Returns true if we should insert this file.
    bool checkPath(
            const std::string& localPath,
            Origin origin,
            const FileInfo& info);

    // Insert points from a file.  Return true if successful.  Sets any
    // previously unset FileInfo fields based on file contents.
    bool insertPath(Origin origin, FileInfo& info);

    // Returns a stack of rejected info nodes so that they may be reused.
    PooledInfoStack insertData(
            PooledInfoStack infoStack,
            const Origin origin,
            Clipper* clipper);

    // Remove resources that are no longer needed.
    void clip(const Id& index, std::size_t chunkNum, Clipper* clipper);

    // Awaken the tree from a saved state.
    void load(std::size_t clipThreads, std::size_t subsetId = 0);

    // Validate sources.
    void prep();

    // Set up bookkeeping, for example initializing the SRS.
    void init();

    // Callers of these functions must not hold a lock on m_mutex.
    Origin end() const;
    bool keepGoing() const;
    void next();

    // Ensure that the file at this path is accessible locally for execution.
    // Return the local path.
    std::string localize(std::string path, Origin origin);

    // Get metadata properties, and load from those serialized properties.
    Json::Value saveProps() const;
    void loadProps(const Json::Value& props);

    //

    std::unique_ptr<BBox> m_bbox;
    std::unique_ptr<BBox> m_subBBox;
    std::unique_ptr<Schema> m_schema;
    std::unique_ptr<Structure> m_structure;
    std::unique_ptr<Manifest> m_manifest;
    std::unique_ptr<Subset> m_subset;

    std::unique_ptr<Reprojection> m_reprojection;

    mutable std::mutex m_mutex;

    bool m_compress;
    bool m_trustHeaders;
    bool m_isContinuation;
    std::string m_srs;

    std::unique_ptr<Pool> m_pool;
    std::unique_ptr<Executor> m_executor;

    pdal::Dimension::Id::Enum m_originId;
    Origin m_origin;
    Origin m_end;

    std::shared_ptr<arbiter::Arbiter> m_arbiter;
    std::unique_ptr<arbiter::Endpoint> m_outEndpoint;
    std::unique_ptr<arbiter::Endpoint> m_tmpEndpoint;

    mutable std::unique_ptr<Pools> m_pointPool;
    std::unique_ptr<Registry> m_registry;

    Builder(const Builder&);
    Builder& operator=(const Builder&);
};

} // namespace entwine


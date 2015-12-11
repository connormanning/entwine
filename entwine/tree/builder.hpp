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

#include <entwine/third/splice-pool/splice-pool.hpp>
#include <entwine/tree/manifest.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/tree/registry.hpp>
#include <entwine/types/dim-info.hpp>
#include <entwine/types/range.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/pool.hpp>

namespace pdal
{
    class PointView;
}

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
class Registry;
class Reprojection;
class SimplePointTable;

class Builder
{
    friend class Clipper;

public:
    Builder(
            std::unique_ptr<Manifest> manifest,
            std::string outPath,
            std::string tmpPath,
            bool compress,
            bool trustHeaders,
            const Reprojection* reprojection,
            const BBox* bbox,
            const DimList& dimList,
            std::size_t numThreads,
            const Structure& structure,
            std::shared_ptr<arbiter::Arbiter> arbiter = 0);

    Builder(
            std::unique_ptr<Manifest> manifest,
            std::string outPath,
            std::string tmpPath,
            std::size_t numThreads,
            std::shared_ptr<arbiter::Arbiter> arbiter = 0);

    // Only used for merging.
    Builder(std::string path, std::shared_ptr<arbiter::Arbiter> arbiter = 0);

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
    const BBox* bbox() const                    { return m_bbox.get(); }
    const BBox* subBBox() const                 { return m_subBBox.get(); }
    const Schema& schema() const                { return *m_schema; }
    const Structure& structure() const          { return *m_structure; }
    const Reprojection* reprojection() const    { return m_reprojection.get(); }
    const Manifest& manifest() const            { return *m_manifest; }

    bool compress() const       { return m_compress; }
    bool trustHeaders() const   { return m_trustHeaders; }
    bool isContinuation() const { return m_isContinuation; }

    const std::string& srs() const { return m_srs; }
    std::size_t numThreads() const { return m_pool->numThreads(); }

    const arbiter::Endpoint& outEndpoint() const { return *m_outEndpoint; }
    const arbiter::Endpoint& tmpEndpoint() const { return *m_tmpEndpoint; }

    bool setEnd(Origin end);

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

    // Insert chunked points from a PointView.
    void insertView(
            SimplePointTable& table,
            Origin origin,
            Clipper* clipper);

    // Remove resources that are no longer needed.
    void clip(const Id& index, std::size_t chunkNum, Clipper* clipper)
    {
        m_registry->clip(index, chunkNum, clipper);
    }

    // Awaken the tree from a saved state.
    void load(std::size_t clipThreads);

    // Validate sources.
    void prep();

    // Set up bookkeeping, for example initializing the SRS.
    void init();

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
    std::unique_ptr<Reprojection> m_reprojection;
    std::unique_ptr<Manifest> m_manifest;

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

    std::unique_ptr<Pools> m_pointPool;
    std::unique_ptr<Registry> m_registry;

    Builder(const Builder&);
    Builder& operator=(const Builder&);
};

} // namespace entwine


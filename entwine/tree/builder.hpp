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
#include <string>
#include <vector>

#include <pdal/Dimension.hpp>

#include <entwine/drivers/source.hpp>
#include <entwine/tree/manifest.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/types/dim-info.hpp>
#include <entwine/types/stats.hpp>
#include <entwine/types/structure.hpp>

namespace pdal
{
    class PointView;
}

namespace Json
{
    class Value;
}

namespace entwine
{

class Arbiter;
class BBox;
class Clipper;
class Driver;
class Executor;
class Pool;
class Registry;
class Reprojection;

class Builder
{
public:
    Builder(
            std::string outPath,
            std::string tmpPath,
            const Reprojection* reprojection,
            const BBox* bbox,
            const DimList& dimList,
            std::size_t numThreads,
            const Structure& structure,
            std::shared_ptr<Arbiter> arbiter = 0);

    Builder(
            std::string buildPath,
            std::string tmpPath,
            std::size_t numThreads,
            std::shared_ptr<Arbiter> arbiter = 0);

    ~Builder();

    // Insert the points from a PointView into this index asynchronously.
    // Returns true if this file has not already been inserted, and this file
    // has been successfully identified as a readable point cloud file.  When
    // this function returns true, the file will be enqueued for point
    // insertion into the index.
    bool insert(std::string filename);

    // Remove resources that are no longer needed.
    void clip(std::size_t index, Clipper* clipper);

    // Save the current state of the tree.
    void save();

    // Block until all running insertion tasks are finished.
    void join();

    Stats stats() const;

private:
    // Awaken the tree from a saved state.  After a load(), no queries should
    // be made until save() is subsequently called.
    void load();

    // Validate sources.
    void prep();

    // Ensure that the file at this path is accessible locally for execution.
    // Return the local path.
    std::string localize(std::string path, Origin origin);

    // Initialize our bounds from a path.
    void inferBBox(std::string path);

    // Insert each point from a pdal::PointView into the Registry.
    void insert(pdal::PointView& pointView, Origin origin, Clipper* clipper);

    // Get metadata properties, and load from those serialized properties.
    Json::Value saveProps() const;
    void loadProps(const Json::Value& props);

    std::string name() const;

    //

    std::unique_ptr<BBox> m_bbox;
    std::unique_ptr<Schema> m_schema;
    std::unique_ptr<Structure> m_structure;
    std::unique_ptr<Reprojection> m_reprojection;
    std::unique_ptr<Manifest> m_manifest;

    Stats m_stats;

    std::unique_ptr<Pool> m_pool;
    std::unique_ptr<Executor> m_executor;

    pdal::Dimension::Id::Enum m_originId;

    std::shared_ptr<Arbiter> m_arbiter;
    Source m_outSource;
    Source m_tmpSource;

    std::unique_ptr<Registry> m_registry;

    Builder(const Builder&);
    Builder& operator=(const Builder&);
};

} // namespace entwine


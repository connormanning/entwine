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

namespace pdal
{
    class PointView;
    class StageFactory;
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
class Pool;
class Registry;
class Reprojection;

class Builder
{
public:
    Builder(
            std::string buildPath,
            std::string tmpPath,
            const Reprojection* reprojection,
            const BBox& bbox,
            const DimList& dimList,
            std::size_t numThreads,
            std::size_t numDimensions,
            std::size_t chunkPoints,
            std::size_t baseDepth,
            std::size_t flatDepth,
            std::size_t diskDepth,
            std::shared_ptr<Arbiter> arbiter = 0);

    Builder(
            std::string buildPath,
            std::string tmpPath,
            const Reprojection* reprojection,
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
    void clip(Clipper* clipper, std::size_t index);

    // Save the current state of the tree.
    void save();

    // Write the tree to an export format independent from the specifics of how
    // it was built.
    void finalize(
            std::string path,
            std::size_t chunkPoints,
            std::size_t base,
            bool compress);

    // Block until all current tasks are finished.
    void join();

private:
    // Awaken the tree from a saved state.  After a load(), no queries should
    // be made until save() is subsequently called.
    void load();

    // Validate sources.
    void prep();

    // Insert each point from a pdal::PointView into the Registry.
    void insert(pdal::PointView& pointView, Origin origin, Clipper* clipper);

    // Get metadata properties, and load from those serialized properties.
    Json::Value saveProps() const;
    void loadProps(const Json::Value& props);

    std::string name() const;

    //

    std::unique_ptr<Reprojection> m_reprojection;

    std::unique_ptr<BBox> m_bbox;
    std::unique_ptr<Schema> m_schema;
    pdal::Dimension::Id::Enum m_originId;
    std::size_t m_dimensions;
    std::size_t m_chunkPoints;
    std::size_t m_numPoints;
    std::size_t m_numTossed;

    Manifest m_manifest;
    std::unique_ptr<Pool> m_pool;
    std::shared_ptr<Arbiter> m_arbiter;
    Source m_buildSource;
    Source m_tmpSource;

    std::unique_ptr<pdal::StageFactory> m_stageFactory;
    std::unique_ptr<Registry> m_registry;

    Builder(const Builder&);
    Builder& operator=(const Builder&);
};

} // namespace entwine


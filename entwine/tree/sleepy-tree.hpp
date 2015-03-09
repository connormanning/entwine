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

#include <memory>
#include <vector>

#include <pdal/PointBuffer.hpp>
#include <pdal/Dimension.hpp>

#include <entwine/third/json/json.h>
#include <entwine/http/s3.hpp>
#include <entwine/types/point.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/tree/registry.hpp>

namespace entwine
{

class BBox;
class Schema;

class SleepyTree
{
public:
    SleepyTree(
            const std::string& dir,
            const BBox& bbox,
            const Schema& schema,
            std::size_t baseDepth,
            std::size_t flatDepth,
            std::size_t diskDepth);
    SleepyTree(const std::string& dir);
    ~SleepyTree();

    // Insert the points from a PointBuffer into this index.
    void insert(const pdal::PointBuffer* pointBuffer, Origin origin);

    // Finalize the tree so it may be queried.  No more pipelines may be added.
    void save();

    // Awaken the tree so more pipelines may be added.  After a load(), no
    // queries should be made until save() is subsequently called.
    void load();

    // Get bounds of the quad tree.
    const BBox& getBounds() const;

    // Return all points at depth levels between [depthBegin, depthEnd).
    // A depthEnd value of zero will return all points at levels >= depthBegin.
    MultiResults getPoints(
            std::size_t depthBegin,
            std::size_t depthEnd);

    // Return all points within the bounding box, searching at tree depth
    // levels from [depthBegin, depthEnd).
    // A depthEnd value of zero will return all points within the query range
    // that have a tree level >= depthBegin.
    MultiResults getPoints(
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    pdal::PointContext pointContext() const;

    std::size_t numPoints() const;

private:
    void addMeta(Json::Value& meta) const;
    std::string metaPath() const;

    const std::string m_dir;
    std::unique_ptr<BBox> m_bbox;
    std::unique_ptr<Schema> m_schema;
    std::size_t m_numPoints;

    std::unique_ptr<Registry> m_registry;

    SleepyTree(const SleepyTree&);
    SleepyTree& operator=(const SleepyTree&);
};

} // namespace entwine


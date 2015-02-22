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
#include <pdal/PointContext.hpp>
#include <pdal/Dimension.hpp>

#include "http/s3.hpp"
#include "types/bbox.hpp"
#include "types/point.hpp"
#include "tree/point-info.hpp"
#include "tree/registry.hpp"

class Registry;
struct Schema;

class SleepyTree
{
public:
    SleepyTree(
            const std::string& outPath,
            const BBox& bbox,
            const Schema& schema);
    SleepyTree(const std::string& outPath);
    ~SleepyTree();

    // Insert the points from a PointBuffer into this index.
    void insert(const pdal::PointBuffer* pointBuffer, Origin origin);

    // Finalize the tree so it may be queried.  No more pipelines may be added.
    void save(std::string path = "");

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

    const pdal::PointContext& pointContext() const;
    std::shared_ptr<std::vector<char>> data(uint64_t id);

    std::size_t numPoints() const;

private:
    const std::string m_outPath;
    std::unique_ptr<BBox> m_bbox;

    pdal::PointContext m_pointContext;
    pdal::Dimension::Id::Enum m_originDim;

    std::size_t m_numPoints;

    std::unique_ptr<Registry> m_registry;

    SleepyTree(const SleepyTree&);
    SleepyTree& operator=(const SleepyTree&);
};


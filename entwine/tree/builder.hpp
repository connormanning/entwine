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

class BBox;
class Clipper;
class Pool;
class Registry;
class S3;
struct S3Info;

class Builder
{
public:
    Builder(
            const std::string& path,
            const BBox& bbox,
            const DimList& dimList,
            const S3Info& s3Info,
            std::size_t numThreads,
            std::size_t numDimensions,
            std::size_t baseDepth,
            std::size_t flatDepth,
            std::size_t diskDepth);

    Builder(
            const std::string& path,
            const S3Info& s3Info,
            std::size_t numThreads);

    ~Builder();

    // Insert the points from a PointView into this index asynchronously.  To
    // await the results of all outstanding inserts, call join().
    void insert(const std::string& filename);
    void join();

    // Remove resources that are no longer needed.
    void clip(Clipper* clipper, std::size_t index);

    // Save the current state of the tree.
    void save();

    // Awaken the tree from a saved state.  After a load(), no queries should
    // be made until save() is subsequently called.
    void load();

    // Write the tree to an export format independent from the specifics of how
    // it was built.
    void finalize(const S3Info& s3Info, std::size_t base, bool compress);

    // Get bounds of the quad tree.
    const BBox& getBounds() const;

    // Return all points at depth levels between [depthBegin, depthEnd).
    // A depthEnd value of zero will return all points at levels >= depthBegin.
    std::vector<std::size_t> query(
            Clipper* clipper,
            std::size_t depthBegin,
            std::size_t depthEnd);

    // Return all points within the bounding box, searching at tree depth
    // levels from [depthBegin, depthEnd).
    // A depthEnd value of zero will return all points within the query range
    // that have a tree level >= depthBegin.
    std::vector<std::size_t> query(
            Clipper* clipper,
            const BBox& bbox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    // Get the constituent bytes of a point by its index, with bytes arranged
    // in accordance with the requested schema.  If no point exists at the
    // specified index, returns an empty vector.
    std::vector<char> getPointData(
            Clipper* clipper,
            std::size_t index,
            const Schema& schema);

    const Schema& schema() const;

    std::size_t numPoints() const;
    std::string path() const;
    std::string name() const;

private:
    void insert(
            pdal::PointView& pointView,
            Origin origin,
            Clipper* clipper);

    std::string metaPath() const;
    Json::Value getTreeMeta() const;

    Origin addOrigin(const std::string& remote);
    std::string inferDriver(const std::string& remote) const;
    std::string fetchAndWriteFile(const std::string& remote, Origin origin);

    const std::string m_path;
    std::unique_ptr<BBox> m_bbox;
    std::unique_ptr<Schema> m_schema;
    pdal::Dimension::Id::Enum m_originId;
    std::size_t m_dimensions;
    std::size_t m_numPoints;
    std::size_t m_numTossed;

    std::vector<std::string> m_originList;

    std::unique_ptr<Pool> m_pool;
    std::unique_ptr<pdal::StageFactory> m_stageFactory;
    std::unique_ptr<S3> m_s3;

    std::unique_ptr<Registry> m_registry;

    Builder(const Builder&);
    Builder& operator=(const Builder&);
};

} // namespace entwine


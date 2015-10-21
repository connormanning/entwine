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

#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include <entwine/types/bbox.hpp>
#include <entwine/types/structure.hpp>

namespace pdal
{
    class BasePointTable;
    class Filter;
    class PointView;
    class Reader;
    class StageFactory;
}

namespace entwine
{

class Reprojection;
class Schema;
class SimplePointTable;

class Preview
{
public:
    Preview(const BBox& bbox, std::size_t numPoints, const std::string& srs)
        : bbox(bbox)
        , numPoints(numPoints)
        , srs(srs)
    { }

    BBox bbox;
    std::size_t numPoints;
    std::string srs;
};

class Executor
{
public:
    Executor(bool is3d);
    ~Executor();

    // Returns true if no errors occurred during insertion.
    bool run(
            SimplePointTable& pointTable,
            std::string path,
            const Reprojection* reprojection,
            std::function<void(pdal::PointView&)> f);

    // True if this path is recognized as a point cloud file.
    bool good(std::string path) const;

    // If available, return the bounds specified in the file header without
    // reading the whole file.
    std::unique_ptr<Preview> preview(
            std::string path,
            const Reprojection* reprojection,
            bool doSrs = false);

private:
    std::unique_ptr<pdal::Reader> createReader(
            std::string driver,
            std::string path) const;

    std::unique_ptr<pdal::Filter> createReprojectionFilter(
            const Reprojection& reprojection,
            pdal::BasePointTable& pointTable) const;

    std::unique_lock<std::mutex> getLock() const;

    bool m_is3d;
    std::unique_ptr<pdal::StageFactory> m_stageFactory;
    mutable std::mutex m_factoryMutex;
};

} // namespace entwine


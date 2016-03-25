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
    class Stage;
    class StageFactory;
}

namespace entwine
{

class PooledPointTable;
class Reprojection;
class Schema;

class ScopedStage
{
public:
    explicit ScopedStage(
            pdal::Stage* stage,
            pdal::StageFactory& stageFactory,
            std::mutex& factoryMutex);

    ~ScopedStage();

    template<typename T> T getAs() { return static_cast<T>(m_stage); }

private:
    pdal::Stage* m_stage;
    pdal::StageFactory& m_stageFactory;
    std::mutex& m_factoryMutex;
};

typedef std::unique_ptr<ScopedStage> UniqueStage;

class Preview
{
public:
    Preview(
            const BBox& bbox,
            std::size_t numPoints,
            const std::string& srs,
            const std::vector<std::string>& dimNames)
        : bbox(bbox)
        , numPoints(numPoints)
        , srs(srs)
        , dimNames(dimNames)
    { }

    BBox bbox;
    std::size_t numPoints;
    std::string srs;
    std::vector<std::string> dimNames;
};

class Executor
{
public:
    Executor(bool is3d);
    ~Executor();

    // Returns true if no errors occurred during insertion.
    bool run(
            PooledPointTable& table,
            std::string path,
            const Reprojection* reprojection);

    // True if this path is recognized as a point cloud file.
    bool good(std::string path) const;

    // If available, return the bounds specified in the file header without
    // reading the whole file.
    std::unique_ptr<Preview> preview(
            std::string path,
            const Reprojection* reprojection);

    std::unique_lock<std::mutex> getLock() const;

private:
    UniqueStage createReader(std::string path) const;
    UniqueStage createReprojectionFilter(const Reprojection& r) const;

    bool m_is3d;
    std::unique_ptr<pdal::StageFactory> m_stageFactory;
    mutable std::mutex m_factoryMutex;
};

} // namespace entwine


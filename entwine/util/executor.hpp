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

#include <entwine/types/bounds.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/unique.hpp>

namespace pdal
{
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
            const Bounds& bounds,
            std::size_t numPoints,
            const std::string& srs,
            const std::vector<std::string>& dimNames,
            const Scale* scale,
            const Json::Value& metadata)
        : bounds(bounds)
        , numPoints(numPoints)
        , srs(srs)
        , dimNames(dimNames)
        , scale(maybeClone(scale))
        , metadata(metadata)
    { }

    Bounds bounds;
    std::size_t numPoints;
    std::string srs;
    std::vector<std::string> dimNames;
    std::unique_ptr<Scale> scale;
    Json::Value metadata;
};

class Executor
{
public:
    Executor();
    ~Executor();

    // Returns true if no errors occurred during insertion.
    bool run(
            PooledPointTable& table,
            std::string path,
            const Reprojection* reprojection,
            const std::vector<double>* transform = nullptr);

    // True if this path is recognized as a point cloud file.
    bool good(std::string path) const;

    // If available, return the bounds specified in the file header without
    // reading the whole file.
    std::unique_ptr<Preview> preview(
            std::string path,
            const Reprojection* reprojection);

    std::string getSrsString(std::string input) const;

    Bounds transform(
            const Bounds& bounds,
            const Transformation& transformation) const;

private:
    UniqueStage createReader(std::string path) const;
    UniqueStage createReprojectionFilter(const Reprojection& r) const;
    UniqueStage createTransformationFilter(const std::vector<double>& m) const;

    std::unique_lock<std::mutex> getLock() const;

    std::unique_ptr<pdal::StageFactory> m_stageFactory;
    mutable std::mutex m_factoryMutex;
};

} // namespace entwine


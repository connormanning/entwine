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

class FsDriver;
class Reprojection;
class Schema;

class Executor
{
public:
    Executor(const Schema& schema);
    ~Executor();

    // Returns true if no errors occurred during insertion.
    bool run(
            std::string path,
            const Reprojection* reprojection,
            std::function<void(pdal::PointView&)> f);

    // True if this path is recognized as a point cloud file.
    bool good(std::string path) const;

private:
    std::unique_ptr<pdal::Reader> createReader(
            std::string driver,
            std::string path) const;

    std::shared_ptr<pdal::Filter> createReprojectionFilter(
            const Reprojection& reprojection,
            pdal::BasePointTable& pointTable) const;

    std::unique_lock<std::mutex> getLock() const;

    const Schema& m_schema;
    std::unique_ptr<pdal::StageFactory> m_stageFactory;
    std::unique_ptr<FsDriver> m_fs;

    mutable std::mutex m_factoryMutex;
};

} // namespace entwine


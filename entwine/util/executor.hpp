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

#include <pdal/Filter.hpp>
#include <pdal/Reader.hpp>

#include <entwine/types/bounds.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace pdal
{
    class StageFactory;
}

namespace entwine
{

class ScopedStage
{
public:
    explicit ScopedStage(
            pdal::Stage* stage,
            pdal::StageFactory& stageFactory,
            std::mutex& factoryMutex);

    ~ScopedStage();

    pdal::Stage* get() { return m_stage; }

private:
    pdal::Stage* m_stage;
    pdal::StageFactory& m_stageFactory;
    std::mutex& m_factoryMutex;
};

typedef std::unique_ptr<ScopedStage> UniqueStage;

class ScanInfo
{
public:
    ScanInfo() = default;
    ScanInfo(pdal::Stage& reader, const pdal::QuickInfo& qi);
    static std::unique_ptr<ScanInfo> create(pdal::Stage& reader);

    std::string srs;
    std::unique_ptr<Scale> scale;
    json metadata;

    Bounds bounds;
    std::size_t points = 0;
    std::vector<std::string> dimNames;
};

class Executor
{
public:
    static Executor& get()
    {
        static Executor e;
        return e;
    }

    static std::mutex& mutex()
    {
        return get().m_mutex;
    }

    // True if this path is recognized as a point cloud file.
    bool good(std::string path) const;

    bool run(pdal::StreamPointTable& table, const Json::Value& pipeline);

    std::unique_ptr<ScanInfo> preview(
            Json::Value pipeline,
            bool trustHeaders = true) const;

    static std::unique_lock<std::mutex> getLock();

private:
    std::unique_ptr<ScanInfo> deepScan(Json::Value pipeline) const;

    Executor();
    ~Executor();

    Executor(const Executor&) = delete;
    Executor& operator=(const Executor&) = delete;

    mutable std::mutex m_mutex;
    std::unique_ptr<pdal::StageFactory> m_stageFactory;
};

} // namespace entwine


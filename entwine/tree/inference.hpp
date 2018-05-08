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
#include <mutex>
#include <set>
#include <string>

#include <pdal/SpatialReference.hpp>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/config.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/file-info.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

class Builder;
class Reprojection;

class NewInference
{
public:
    NewInference(const Config& config);
    Config go();

private:
    void add(FileInfo& f);
    void add(FileInfo& f, std::string localPath);
    Config aggregate();

    const Config m_in;

    bool m_done = false;
    std::unique_ptr<Pool> m_pool;
    std::size_t m_index = 0;
    arbiter::Arbiter m_arbiter;
    arbiter::Endpoint m_tmp;
    std::unique_ptr<Reprojection> m_re;
    mutable std::mutex m_mutex;

    // These are the portions we build during go().
    FileInfoList m_fileInfo;
    Schema m_schema;
    Scale m_scale = 1;
};

class Inference
{
public:
    Inference(const Config& config);

    Inference(
            const FileInfoList& fileInfo,
            const Reprojection* reprojection = nullptr,
            bool trustHeaders = true,
            bool allowDelta = true,
            std::string tmpPath = ".",
            std::size_t threads = 4,
            bool verbose = false,
            bool cesiumify = false,
            arbiter::Arbiter* arbiter = nullptr);

    // For API convenience.
    Inference(
            const Paths& paths,
            const Reprojection* reprojection = nullptr,
            bool trustHeaders = true,
            bool allowDelta = true,
            std::string tmpPath = ".",
            std::size_t threads = 4,
            bool verbose = false,
            bool cesiumify = false,
            arbiter::Arbiter* arbiter = nullptr);

    Inference(
            std::string path,
            const Reprojection* reprojection = nullptr,
            bool trustHeaders = true,
            bool allowDelta = true,
            std::string tmpPath = ".",
            std::size_t threads = 4,
            bool verbose = false,
            bool cesiumify = false,
            arbiter::Arbiter* arbiter = nullptr);

    Inference(Builder& builder, const FileInfoList& fileInfo);

    Inference(const Json::Value& json);
    ~Inference();

    void go();
    bool done() const { return m_done; }

    std::size_t index() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_index;
    }

    const FileInfoList& fileInfo() const { return m_fileInfo; }
    Schema schema() const;
    Bounds bounds() const;
    std::size_t numPoints() const;
    const Reprojection* reprojection() const { return m_reproj.get(); }
    const Delta* delta() const { return m_delta.get(); }

    const std::vector<double>* transformation() const
    {
        return m_transformation.get();
    }

    void transformation(std::vector<double> t)
    {
        m_transformation = makeUnique<std::vector<double>>(t);
    }

    Json::Value toJson() const;

private:
    void aggregate();   // Aggregate bounds and numPoints.
    void makeSchema();  // Figure out schema and delta.
    void check() const; // Verify that our inference is valid - otherwise throw.

    void add(std::string localPath, FileInfo& fileInfo);
    Transformation calcTransformation();

    std::string m_tmpPath;

    PointPool m_pointPool;
    std::unique_ptr<Reprojection> m_reproj;
    std::size_t m_threads = 4;
    bool m_verbose = true;
    bool m_trustHeaders = true;
    bool m_allowDelta = true;
    bool m_valid = false;
    bool m_done = false;
    bool m_cesiumify = false;
    std::unique_ptr<Transformation> m_transformation;

    std::unique_ptr<Pool> m_pool;
    std::unique_ptr<arbiter::Arbiter> m_ownedArbiter;
    arbiter::Arbiter* m_arbiter;
    arbiter::Endpoint m_tmp;
    std::size_t m_index = 0;

    std::vector<std::string> m_dimVec;
    std::set<std::string> m_dimSet;

    std::unique_ptr<std::size_t> m_numPoints;
    std::unique_ptr<Bounds> m_bounds;
    std::unique_ptr<Schema> m_schema;
    std::unique_ptr<Delta> m_delta;

    FileInfoList m_fileInfo;

    mutable std::mutex m_mutex;
};

} // namespace entwine


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

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/manifest.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/executor.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

class Executor;
class Reprojection;

class Inference
{
public:
    Inference(
            std::string path,
            std::string tmpPath,
            std::size_t threads,
            bool verbose = false,
            const Reprojection* reprojection = nullptr,
            bool trustHeaders = true,
            bool allowDelta = true,
            arbiter::Arbiter* arbiter = nullptr);

    Inference(
            const Manifest& manifest,
            std::string tmpPath,
            std::size_t threads,
            bool verbose = false,
            const Reprojection* reprojection = nullptr,
            bool trustHeaders = true,
            bool allowDelta = true,
            arbiter::Arbiter* arbiter = nullptr,
            bool cesiumify = false);

    void go();
    bool done() const { return m_done; }

    std::size_t index() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_index;
    }

    const Manifest& manifest() const { return m_manifest; }
    Schema schema() const;
    Bounds nativeBounds() const;
    std::size_t numPoints() const;
    const Reprojection* reprojection() const { return m_reproj; }
    const Delta* delta() const { return m_delta.get(); }
    const Bounds* deltaBounds() const { return m_deltaBounds.get(); }

    const std::vector<double>* transformation() const
    {
        return m_transformation.get();
    }

private:
    void aggregate();   // Aggregate bounds and numPoints.
    void makeSchema();  // Figure out schema and delta.

    void add(std::string localPath, FileInfo& fileInfo);
    Transformation calcTransformation();

    Executor m_executor;

    std::string m_path;
    std::string m_tmpPath;

    PointPool m_pointPool;
    const Reprojection* m_reproj;
    std::size_t m_threads;
    bool m_verbose;
    bool m_trustHeaders;
    bool m_allowDelta;
    bool m_done;

    std::unique_ptr<Pool> m_pool;
    std::unique_ptr<arbiter::Arbiter> m_ownedArbiter;
    arbiter::Arbiter* m_arbiter;
    arbiter::Endpoint m_tmp;
    Manifest m_manifest;
    std::size_t m_index;

    std::vector<std::string> m_dimVec;
    std::set<std::string> m_dimSet;

    std::unique_ptr<std::size_t> m_numPoints;
    std::unique_ptr<Bounds> m_bounds;
    std::unique_ptr<Schema> m_schema;
    std::unique_ptr<Delta> m_delta;
    std::unique_ptr<Bounds> m_deltaBounds;

    bool m_cesiumify;
    std::unique_ptr<Transformation> m_transformation;

    mutable std::mutex m_mutex;
};

} // namespace entwine


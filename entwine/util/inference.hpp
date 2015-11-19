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
#include <entwine/types/bbox.hpp>
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
            arbiter::Arbiter* arbiter = nullptr);

    Inference(
            const Manifest& manifest,
            std::string tmpPath,
            std::size_t threads,
            bool verbose = false,
            const Reprojection* reprojection = nullptr,
            bool trustHeaders = true,
            arbiter::Arbiter* arbiter = nullptr);

    void go();
    bool done() const { return m_done; }

    std::size_t index() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_index;
    }

    const Manifest& manifest() const { return m_manifest; }
    Schema schema() const;
    BBox bbox() const;

private:
    void add(std::string localPath, FileInfo& fileInfo);

    Executor m_executor;
    DataPool m_dataPool;
    const Reprojection* m_reproj;
    std::size_t m_threads;
    bool m_verbose;
    bool m_trustHeaders;
    bool m_done;

    std::unique_ptr<Pool> m_pool;
    std::unique_ptr<arbiter::Arbiter> m_ownedArbiter;
    arbiter::Arbiter* m_arbiter;
    arbiter::Endpoint m_tmp;
    Manifest m_manifest;
    std::size_t m_index;

    std::vector<std::string> m_dimVec;
    std::set<std::string> m_dimSet;

    mutable std::mutex m_mutex;
};

} // namespace entwine


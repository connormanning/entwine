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

#include <entwine/types/bbox.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/executor.hpp>

namespace arbiter
{
    class Arbiter;
}

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
            bool cubeify = true,
            arbiter::Arbiter* arbiter = nullptr);

    std::size_t numPoints() const { return m_numPoints; }
    BBox bbox() const { return m_bbox; }
    Schema schema() const;

private:
    void add(std::string localPath, std::size_t index);

    Executor m_executor;
    DataPool m_dataPool;
    const Reprojection* m_reproj;
    bool m_verbose;
    bool m_trustHeaders;
    bool m_cubeify;

    std::size_t m_numPoints;
    BBox m_bbox;
    std::vector<std::string> m_dimVec;
    std::set<std::string> m_dimSet;

    std::mutex m_mutex;
};

} // namespace entwine


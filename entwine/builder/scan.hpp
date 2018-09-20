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

#include <entwine/builder/config.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/file-info.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

class Reprojection;

class Scan
{
public:
    Scan(Config config);
    Config go();

    const Config& inConfig() const { return m_in; }

    std::size_t index() const { return m_index; }
    std::size_t total() const { return m_fileInfo.size(); }

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

} // namespace entwine


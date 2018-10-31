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
#include <string>
#include <vector>

#include <entwine/builder/config.hpp>
#include <entwine/util/pool.hpp>

namespace Json { class Value; }

namespace entwine
{

namespace arbiter { class Arbiter; }

class Builder;

class Merger
{
public:
    Merger(const Config& config);
    ~Merger();

    void go();

    uint64_t id() const { return m_id; }
    uint64_t of() const { return m_of; }

private:
    const Config m_config;
    std::unique_ptr<Builder> m_builder;
    std::shared_ptr<arbiter::Arbiter> m_arbiter;

    uint64_t m_id = 1;
    uint64_t m_of = 0;
    bool m_verbose;

    uint64_t m_threads;
    Pool m_pool;
};

} // namespace entwine


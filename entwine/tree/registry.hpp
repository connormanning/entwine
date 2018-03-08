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
#include <vector>

#include <json/json.h>

#include <entwine/tree/chunk.hpp>
#include <entwine/tree/cold.hpp>
#include <entwine/tree/slice.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/tube.hpp>
#include <entwine/util/unique.hpp>

namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class NewClimber;
class NewClipper;
class Structure;

class Registry
{
    friend class Builder;

public:
    Registry(
            const Metadata& metadata,
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pointPool,
            bool exists = false);
    ~Registry();

    void save(const arbiter::Endpoint& endpoint) const;
    void merge(const Registry& other) { } // TODO

    bool addPoint(
            Cell::PooledNode& cell,
            NewClimber& climber,
            NewClipper& clipper,
            std::size_t maxDepth = 0);

    void clip(uint64_t d, uint64_t x, uint64_t y, uint64_t o);

    const Metadata& metadata() const { return m_metadata; }

private:
    void loadAsNew();
    void loadFromRemote();

    void hierarchy(
            Json::Value& json,
            uint64_t d,
            uint64_t x,
            uint64_t y) const;

    const Metadata& m_metadata;
    const arbiter::Endpoint& m_out;
    const arbiter::Endpoint& m_tmp;

    std::vector<Slice> m_slices;
};

} // namespace entwine


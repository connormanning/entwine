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

#include <entwine/third/json/json.hpp>
#include <entwine/types/point-pool.hpp>

namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class BaseChunk;
class Builder;
class Climber;
class Clipper;
class Cold;
class Structure;

class Registry
{
    friend class Builder;

public:
    Registry(
            arbiter::Endpoint& endpoint,
            const Builder& builder,
            std::size_t clipPoolSize);

    Registry(
            arbiter::Endpoint& endpoint,
            const Builder& builder,
            std::size_t clipPoolSize,
            const Json::Value& meta);

    Json::Value toJson() const;
    void merge(const Registry& other);

    ~Registry();

    bool addPoint(
            Cell::PooledNode& cell,
            Climber& climber,
            Clipper& clipper,
            std::size_t maxDepth = 0);

    void clip(const Id& index, std::size_t chunkNum, std::size_t id);

    std::set<Id> ids() const;

private:
    bool insert(
            const Climber& climber,
            Clipper& clipper,
            Cell::PooledNode& cell);

    BaseChunk* base() { return m_base.get(); }
    Cold* cold() { return m_cold.get(); }

    arbiter::Endpoint& m_endpoint;
    const Builder& m_builder;
    const Structure& m_structure;

    const bool m_discardDuplicates;
    const bool m_as3d;

    std::unique_ptr<BaseChunk> m_base;
    std::unique_ptr<Cold> m_cold;
};

} // namespace entwine


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

#include <entwine/tree/point-info.hpp>

namespace Json
{
    class Value;
}

namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class BaseChunk;
class Builder;
class Cell;
class Climber;
class Clipper;
class Cold;
class Structure;

class Registry
{
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

    void addClipWorker();
    void delClipWorker();
    std::size_t clipThreads() const;

    bool addPoint(
            PooledInfoNode& toAdd,
            Climber& climber,
            Clipper& clipper);

    Cell* getCell(const Climber& climber, Clipper& clipper);

    void save();
    void clip(const Id& index, std::size_t chunkNum, std::size_t id);

    std::set<Id> ids() const;

private:
    arbiter::Endpoint& m_endpoint;
    const Builder& m_builder;
    const Structure& m_structure;

    const bool m_discardDuplicates;
    const bool m_as3d;

    std::unique_ptr<BaseChunk> m_base;
    std::unique_ptr<Cold> m_cold;
};

} // namespace entwine


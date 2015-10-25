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
#include <vector>

#include <entwine/types/structure.hpp>

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

class Cell;
class Chunk;
class Climber;
class Clipper;
class Cold;
class BaseChunk;
class PointInfo;
class Pool;
class Schema;

class Registry
{
public:
    Registry(
            arbiter::Endpoint& endpoint,
            const Schema& schema,
            const BBox& bbox,
            const Structure& structure,
            Pools& pointPool,
            std::size_t clipPoolSize);

    Registry(
            arbiter::Endpoint& endpoint,
            const Schema& schema,
            const BBox& bbox,
            const Structure& structure,
            Pools& pointPool,
            std::size_t clipPoolSize,
            const Json::Value& meta);

    ~Registry();

    bool addPoint(
            PooledInfoNode& toAdd,
            Climber& climber,
            Clipper* clipper);

    Cell* getCell(const Climber& climber, Clipper* clipper);

    void save(Json::Value& meta);
    void clip(const Id& index, std::size_t chunkNum, Clipper* clipper);

private:
    arbiter::Endpoint& m_endpoint;
    const Schema& m_schema;
    const BBox& m_bbox;
    const Structure& m_structure;
    Pools& m_pointPool;

    bool m_is3d;

    std::unique_ptr<BaseChunk> m_base;
    std::unique_ptr<Cold> m_cold;
    std::unique_ptr<Pool> m_pool;
};

} // namespace entwine


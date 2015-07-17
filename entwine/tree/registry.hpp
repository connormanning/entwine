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

class Chunk;
class Climber;
class Clipper;
class Cold;
class ContiguousChunkData;
class Entry;
class PointInfo;
class Pool;
class Schema;
class Structure;

class Registry
{
public:
    Registry(
            arbiter::Endpoint& endpoint,
            const Schema& schema,
            const Structure& structure);

    Registry(
            arbiter::Endpoint& endpoint,
            const Schema& schema,
            const Structure& structure,
            const Json::Value& meta);

    ~Registry();

    bool addPoint(PointInfo& toAdd, Climber& climber, Clipper* clipper);

    void save(Json::Value& meta);

    Entry* getEntry(const Climber& climber, Clipper* clipper);

    void clip(std::size_t index, Clipper* clipper);

private:
    arbiter::Endpoint & m_endpoint;
    const Schema& m_schema;
    const Structure& m_structure;
    bool m_is3d;

    std::unique_ptr<ContiguousChunkData> m_base;
    std::unique_ptr<Cold> m_cold;

    std::unique_ptr<Pool> m_pool;

    const std::vector<char> m_empty;
};

} // namespace entwine


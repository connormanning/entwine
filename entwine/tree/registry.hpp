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
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/cold.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/tube.hpp>

namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class Builder;
class Climber;
class Clipper;
class Structure;

class Registry
{
public:
    Registry(const Builder& builder, bool exists = false);
    ~Registry();

    void save() const;
    void merge(const Registry& other);

    bool addPoint(
            Cell::PooledNode& cell,
            Climber& climber,
            Clipper& clipper,
            std::size_t maxDepth = 0);

    void clip(const Id& index, std::size_t chunkNum, std::size_t id);

    std::set<Id> ids() const;

private:
    void loadAsNew();
    void loadFromRemote();

    Tube::Insertion insert(
            const Climber& climber,
            Clipper& clipper,
            Cell::PooledNode& cell)
    {
        if (m_structure.isWithinBase(climber.depth()))
        {
            return m_base->insert(climber, cell);
        }
        else
        {
            return m_cold->insert(climber, clipper, cell);
        }
    }

    BaseChunk* base() { return m_base.get(); }
    Cold* cold() { return m_cold.get(); }

    const Builder& m_builder;
    const Structure& m_structure;

    std::unique_ptr<BaseChunk> m_base;
    std::unique_ptr<Cold> m_cold;
};

} // namespace entwine


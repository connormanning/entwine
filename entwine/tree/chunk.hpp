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
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include <pdal/PointTable.hpp>

#include <entwine/tree/climber.hpp>
#include <entwine/types/dim-info.hpp>
#include <entwine/types/format.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/tube.hpp>
#include <entwine/util/locker.hpp>

namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class Builder;
class Metadata;

class Chunk
{
    friend class Builder;

public:
    Chunk(
            const Builder& builder,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints);

    virtual ~Chunk();

    static std::unique_ptr<Chunk> create(
            const Builder& builder,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints);

    static std::unique_ptr<Chunk> create(
            const Builder& builder,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints,
            std::unique_ptr<std::vector<char>> data);

    const Id& maxPoints() const { return m_maxPoints; }
    const Id& id() const { return m_id; }

    Tube::Insertion insert(const Climber& climber, Cell::PooledNode& cell)
    {
        return getTube(climber.index()).insert(climber, cell);
    }

    static std::size_t count();

protected:
    void populate(Cell::PooledStack cells);

    void collect(ChunkType type);

    virtual Cell::PooledStack acquire() = 0;
    virtual Tube& getTube(const Id& index) = 0;

    Id endId() const { return m_id + m_maxPoints; }

    const Builder& m_builder;
    const Metadata& m_metadata;
    PointPool& m_pointPool;

    const std::size_t m_depth;
    const std::size_t m_zDepth;
    const Id m_id;

    const Id m_maxPoints;

    std::unique_ptr<std::vector<char>> m_data;
};

class SparseChunk : public Chunk
{
public:
    SparseChunk(
            const Builder& builder,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints);

    SparseChunk(
            const Builder& builder,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints,
            Cell::PooledStack cells);

    virtual ~SparseChunk();

private:
    virtual Cell::PooledStack acquire() override;

    virtual Tube& getTube(const Id& index) override
    {
        const Id norm(normalize(index));

        std::lock_guard<std::mutex> lock(m_mutex);
        return m_tubes[norm];
    }

    Id normalize(const Id& rawIndex) const
    {
        assert(rawIndex >= m_id);
        assert(rawIndex < endId());

        return rawIndex - m_id;
    }

    std::map<Id, Tube> m_tubes;
    std::mutex m_mutex;
};

class ContiguousChunk : public Chunk
{
public:
    ContiguousChunk(
            const Builder& builder,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints);

    ContiguousChunk(
            const Builder& builder,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints,
            Cell::PooledStack cells);

    virtual ~ContiguousChunk();

protected:
    virtual Cell::PooledStack acquire() override;

    virtual Tube& getTube(const Id& index) override
    {
        return m_tubes.at(normalize(index));
    }

    std::size_t normalize(const Id& rawIndex) const
    {
        assert(rawIndex >= m_id);
        assert(rawIndex < endId());

        return (rawIndex - m_id).getSimple();
    }

    std::vector<Tube> m_tubes;
};

class BaseChunk : public ContiguousChunk
{
public:
    BaseChunk(
            const Builder& builder,
            const Id& id,
            const Id& maxPoints);

    BaseChunk(
            const Builder& builder,
            const Id& id,
            const Id& maxPoints,
            Unpacker unpacker);

    // Unlike the other Chunk types, the BaseChunk requires an explicit call to
    // save, rather than serializing during its destructor.
    void save(const arbiter::Endpoint& endpoint);

    Cell::PooledStack acquire() override { return ContiguousChunk::acquire(); }
    void merge(BaseChunk& other);

    static Schema makeCelled(const Schema& in);

private:
    Schema m_celledSchema;
};

} // namespace entwine


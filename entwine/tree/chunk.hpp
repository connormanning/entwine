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
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include <pdal/PointTable.hpp>

#include <entwine/formats/cesium/tile-info.hpp>
#include <entwine/formats/cesium/util.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/types/dim-info.hpp>
#include <entwine/types/format.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/tube.hpp>
#include <entwine/util/locker.hpp>
#include <entwine/util/matrix.hpp>

namespace entwine
{

namespace arbiter { class Endpoint; }

class Builder;
class Metadata;

class Chunk
{
    friend class Builder;

public:
    Chunk(
            const Builder& builder,
            const Bounds& bounds,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints);

    Chunk(Chunk&& other) = default;

    virtual ~Chunk();

    static std::unique_ptr<Chunk> create(
            const Builder& builder,
            const Bounds& bounds,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints);

    static std::unique_ptr<Chunk> create(
            const Builder& builder,
            const Bounds& bounds,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints,
            std::unique_ptr<std::vector<char>> data);

    const Id& maxPoints() const { return m_maxPoints; }
    const Id& id() const { return m_id; }

    Tube::Insertion insert(const Climber& climber, Cell::PooledNode& cell)
    {
        return getTube(climber).insert(climber, cell);
    }

    static std::size_t count();

    virtual cesium::TileInfo info() const = 0;

protected:
    void populate(Cell::PooledStack cells);

    void collect(ChunkType type);

    virtual Cell::PooledStack acquire() = 0;
    virtual Tube& getTube(const Climber& climber) = 0;

    std::size_t divisor() const
    {
        const auto& s(m_metadata.structure());

        std::size_t d(1 << s.nominalChunkDepth());

        if (m_depth > s.sparseDepthBegin())
        {
            d <<= m_depth - s.sparseDepthBegin();
        }

        return d;
    }

    Id endId() const { return m_id + m_maxPoints; }

    virtual void tile() const { }

    const Builder& m_builder;
    const Metadata& m_metadata;
    const Bounds m_bounds;
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
            const Bounds& bounds,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints);

    SparseChunk(
            const Builder& builder,
            const Bounds& bounds,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints,
            Cell::PooledStack cells);

    virtual ~SparseChunk();

    virtual cesium::TileInfo info() const override;

private:
    virtual Cell::PooledStack acquire() override;

    virtual void tile() const override;

    virtual Tube& getTube(const Climber& climber) override
    {
        const Id norm(normalize(climber.index()));

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
    friend class BaseChunk;

public:
    ContiguousChunk(
            const Builder& builder,
            const Bounds& bounds,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints,
            bool autosave = true);

    ContiguousChunk(
            const Builder& builder,
            const Bounds& bounds,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints,
            Cell::PooledStack cells);

    ContiguousChunk(ContiguousChunk&& other) = default;

    virtual ~ContiguousChunk();

    virtual cesium::TileInfo info() const override;

    bool empty() const
    {
        for (const auto& tube : m_tubes)
        {
            if (!tube.empty()) return false;
        }
        return true;
    }

protected:
    virtual Cell::PooledStack acquire() override;

    virtual void tile() const override;

    virtual Tube& getTube(const Climber& climber) override
    {
        return m_tubes.at(normalize(climber.index()));
    }

    std::size_t normalize(const Id& rawIndex) const
    {
        assert(rawIndex >= m_id);
        assert(rawIndex < endId());

        return (rawIndex - m_id).getSimple();
    }

    std::vector<Tube>& tubes() { return m_tubes; }

    std::vector<Tube> m_tubes;
    bool m_autosave;
};

class BaseChunk : public Chunk
{
public:
    BaseChunk(const Builder& builder);
    BaseChunk(const Builder& builder, Unpacker unpacker);

    // Unlike the other Chunk types, the BaseChunk requires an explicit call to
    // save, rather than serializing during its destructor.
    void save(const arbiter::Endpoint& endpoint);

    std::set<Id> merge(BaseChunk& other);

    static Schema makeCelled(const Schema& in);

    virtual cesium::TileInfo info() const override;
    std::vector<cesium::TileInfo> baseInfo() const;

private:
    virtual Cell::PooledStack acquire() override;

    virtual void tile() const override;

    virtual Tube& getTube(const Climber& climber) override
    {
        return m_chunks.at(climber.depth()).getTube(climber);
    }

    void makeWritable();

    std::vector<ContiguousChunk> m_chunks;
    Schema m_celledSchema;

    std::vector<std::vector<ContiguousChunk>> m_writes;
};

} // namespace entwine


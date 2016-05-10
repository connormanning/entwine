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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <pdal/PointTable.hpp>

#include <entwine/tree/cell.hpp>
#include <entwine/types/blocked-data.hpp>
#include <entwine/types/dim-info.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/locker.hpp>

namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class Builder;
class Climber;
class Pools;
class Structure;

class Chunk
{
public:
    Chunk(
            const Builder& builder,
            const BBox& bbox,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints,
            std::size_t numPoints = 0);

    virtual ~Chunk();

    static std::unique_ptr<Chunk> create(
            const Builder& builder,
            const BBox& bbox,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints,
            bool contiguous);

    static std::unique_ptr<Chunk> create(
            const Builder& builder,
            const BBox& bbox,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints,
            std::unique_ptr<std::vector<char>> data);

    enum Type
    {
        Sparse = 0,
        Contiguous,
        Invalid
    };

    struct Tail
    {
        Tail(std::size_t numPoints, Type type)
            : numPoints(numPoints), type(type)
        { }

        uint64_t numPoints;
        Type type;
    };

    static void pushTail(std::vector<char>& data, Tail tail);
    static Tail popTail(std::vector<char>& data);

    static std::size_t getChunkMem();
    static std::size_t getChunkCnt();

    const Id& maxPoints() const { return m_maxPoints; }
    const Id& id() const { return m_id; }

    virtual void save(arbiter::Endpoint& endpoint) = 0;
    virtual Cell& getCell(const Climber& climber) = 0;

protected:
    Id endId() const { return m_id + m_maxPoints; }

    const Builder& m_builder;
    const BBox m_bbox;
    const std::size_t m_zDepth;
    const Id m_id;

    const Id m_maxPoints;
    std::atomic_size_t m_numPoints;
};

class SparseChunk : public Chunk
{
public:
    SparseChunk(
            const Builder& builder,
            const BBox& bbox,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints);

    SparseChunk(
            const Builder& builder,
            const BBox& bbox,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints,
            std::unique_ptr<std::vector<char>> compressedData,
            std::size_t numPoints);

    ~SparseChunk();

    virtual void save(arbiter::Endpoint& endpoint) override;
    virtual Cell& getCell(const Climber& climber) override;

private:
    Id normalize(const Id& rawIndex) const
    {
        assert(rawIndex >= m_id);
        assert(rawIndex < endId());

        return rawIndex - m_id;
    }

    std::unordered_map<Id, Tube> m_tubes;
    std::mutex m_mutex;
};

class ContiguousChunk : public Chunk
{
public:
    ContiguousChunk(
            const Builder& builder,
            const BBox& bbox,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints);

    ContiguousChunk(
            const Builder& builder,
            const BBox& bbox,
            std::size_t depth,
            const Id& id,
            const Id& maxPoints,
            std::unique_ptr<std::vector<char>> compressedData,
            std::size_t numPoints);

    ~ContiguousChunk();

    virtual void save(arbiter::Endpoint& endpoint) override;
    virtual Cell& getCell(const Climber& climber) override;

    const Tube& getTube(const Id& index) const
    {
        return m_tubes.at(normalize(index));
    }

protected:
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
            const BBox& bbox,
            const Id& id,
            const Id& maxPoints);

    BaseChunk(
            const Builder& builder,
            const BBox& bbox,
            const Id& id,
            const Id& maxPoints,
            std::unique_ptr<std::vector<char>> compressedData,
            std::size_t numPoints);

    virtual void save(arbiter::Endpoint& endpoint) override;

    PooledInfoStack acquire(InfoPool& infoPool);
    void merge(BaseChunk& other);

    static Schema makeCelled(const Schema& in);

private:
    Schema m_celledSchema;
};

} // namespace entwine


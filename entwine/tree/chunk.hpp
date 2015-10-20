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

#include <entwine/tree/cell.hpp>
#include <entwine/types/blocked-data.hpp>
#include <entwine/types/dim-info.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/util/locker.hpp>

namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class Climber;
class Schema;

class Chunk
{
public:
    Chunk(
            const Schema& schema,
            const BBox& bbox,
            const Structure& structure,
            Pools& pools,
            std::size_t depth,
            const Id& id,
            std::size_t maxPoints,
            std::size_t numPoints = 0);

    virtual ~Chunk();

    static std::unique_ptr<Chunk> create(
            const Schema& schema,
            const BBox& bbox,
            const Structure& structure,
            Pools& pools,
            std::size_t depth,
            const Id& id,
            std::size_t maxPoints,
            bool contiguous);

    static std::unique_ptr<Chunk> create(
            const Schema& schema,
            const BBox& bbox,
            const Structure& structure,
            Pools& pools,
            std::size_t depth,
            const Id& id,
            std::size_t maxPoints,
            std::vector<char> data);

    enum Type
    {
        Sparse = 0,
        Contiguous
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

    std::size_t maxPoints() const { return m_maxPoints; }
    const Id& id() const { return m_id; }

    virtual void save(arbiter::Endpoint& endpoint) = 0;
    virtual Cell& getCell(const Climber& climber) = 0;

protected:
    Id endId() const { return m_id + m_maxPoints; }
    std::size_t normalize(const Id& rawIndex) const;

    const Schema& m_schema;
    const BBox m_bbox;
    const Structure& m_structure;
    Pools& m_pools;
    const std::size_t m_depth;
    const std::size_t m_zDepth;
    const Id m_id;

    const std::size_t m_maxPoints;
    std::atomic_size_t m_numPoints;
};

class SparseChunk : public Chunk
{
public:
    SparseChunk(
            const Schema& schema,
            const BBox& bbox,
            const Structure& structure,
            Pools& pools,
            std::size_t depth,
            const Id& id,
            std::size_t maxPoints);

    SparseChunk(
            const Schema& schema,
            const BBox& bbox,
            const Structure& structure,
            Pools& pools,
            std::size_t depth,
            const Id& id,
            std::size_t maxPoints,
            std::vector<char>& compressedData,
            std::size_t numPoints);

    virtual void save(arbiter::Endpoint& endpoint) override;
    virtual Cell& getCell(const Climber& climber) override;

private:
    // TODO This should be an Id for losslessness.  With a uint64, we are
    // limited to 23 sparse depths for a chunk size of 262144.
    std::unordered_map<std::size_t, Tube> m_tubes;
    std::mutex m_mutex;
};

class ContiguousChunk : public Chunk
{
public:
    ContiguousChunk(
            const Schema& schema,
            const BBox& bbox,
            const Structure& structure,
            Pools& pools,
            std::size_t depth,
            const Id& id,
            std::size_t maxPoints);

    ContiguousChunk(
            const Schema& schema,
            const BBox& bbox,
            const Structure& structure,
            Pools& pools,
            std::size_t depth,
            const Id& id,
            std::size_t maxPoints,
            std::vector<char>& compressedData,
            std::size_t numPoints);

    virtual void save(arbiter::Endpoint& endpoint) override;
    virtual Cell& getCell(const Climber& climber) override;

    const Tube& getTube(const Id& index) const
    {
        return m_tubes.at(normalize(index));
    }

protected:
    std::vector<Tube> m_tubes;
};

class BaseChunk : public ContiguousChunk
{
public:
    BaseChunk(
            const Schema& schema,
            const BBox& bbox,
            const Structure& structure,
            Pools& pools,
            const Id& id,
            std::size_t maxPoints);

    BaseChunk(
            const Schema& schema,
            const BBox& bbox,
            const Structure& structure,
            Pools& pools,
            const Id& id,
            std::size_t maxPoints,
            std::vector<char>& compressedData,
            std::size_t numPoints);

    virtual void save(arbiter::Endpoint& endpoint) override;

    void save(arbiter::Endpoint& endpoint, std::string postfix);
    void merge(BaseChunk& other);

private:
    Schema m_celledSchema;
};

} // namespace entwine


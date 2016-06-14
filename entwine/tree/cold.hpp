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
#include <memory>
#include <mutex>
#include <set>
#include <unordered_set>
#include <unordered_map>

#include <entwine/third/json/json.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/tube.hpp>
#include <entwine/util/spin-lock.hpp>


namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class Builder;
class Cell;
class Chunk;
class Climber;
class Clipper;
class Pool;

class Cold
{
public:
    Cold(const Builder& builder, bool exists);
    ~Cold();

    Tube::Insertion insert(
            const Climber& climber,
            Clipper& clipper,
            Cell::PooledNode& cell);

    void save(const arbiter::Endpoint& endpoint) const;
    void clip(const Id& chunkId, std::size_t chunkNum, std::size_t id);

    std::set<Id> ids() const;
    void merge(const Cold& other);

    std::size_t clipThreads() const;

    static std::size_t getNumFastTrackers(const Structure& structure);

private:
    void growFast(const Climber& climber, Clipper& clipper);
    void growSlow(const Climber& climber, Clipper& clipper);
    void growFaux(const Id& other);

    struct CountedChunk
    {
        ~CountedChunk();

        std::unique_ptr<Chunk> chunk;
        std::unordered_map<std::size_t, std::size_t> refs;
        std::mutex mutex;
    };

    void ensureChunk(
            const Climber& climber,
            std::unique_ptr<Chunk>& chunk,
            bool exists);

    void unrefChunk(CountedChunk& countedChunk, std::size_t id, bool fast);

    struct FastSlot
    {
        FastSlot() : mark(false), spinner(), chunk() { }

        std::atomic_bool mark;  // Data exists?
        SpinLock spinner;
        std::unique_ptr<CountedChunk> chunk;
    };

    using ChunkMap = std::unordered_map<Id, std::unique_ptr<CountedChunk>>;

    const Builder& m_builder;
    std::vector<FastSlot> m_chunkVec;
    ChunkMap m_chunkMap;
    std::set<Id> m_fauxIds; // Used for merging, these are added to metadata.

    mutable std::mutex m_mapMutex;
    Pool& m_pool;
};

} // namespace entwine


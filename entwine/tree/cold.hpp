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
#include <entwine/tree/point-info.hpp>

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
    Cold(
            arbiter::Endpoint& endpoint,
            const Builder& builder,
            std::size_t clipPoolSize);

    Cold(
            arbiter::Endpoint& endpoint,
            const Builder& builder,
            std::size_t clipPoolSize,
            const Json::Value& meta);

    ~Cold();

    Cell& getCell(const Climber& climber, Clipper& clipper);

    Json::Value toJson() const;
    void clip(const Id& chunkId, std::size_t chunkNum, std::size_t id);

    std::set<Id> ids() const;
    void merge(const Cold& other);

    std::size_t clipThreads() const;

private:
    void growFast(const Climber& climber, Clipper& clipper);
    void growSlow(const Climber& climber, Clipper& clipper);
    void growFaux(const Id& other);

    struct CountedChunk
    {
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
        FastSlot() : mark(false), flag(), chunk()
        {
            flag.clear();
        }

        std::atomic_bool mark;  // Data exists?
        std::atomic_flag flag;  // Lock.
        std::unique_ptr<CountedChunk> chunk;
    };

    typedef std::unordered_map<Id, std::unique_ptr<CountedChunk>> ChunkMap;

    arbiter::Endpoint& m_endpoint;
    const Builder& m_builder;

    std::vector<FastSlot> m_chunkVec;

    ChunkMap m_chunkMap;
    std::set<Id> m_fauxIds; // Used for merging, these are added to metadata.

    mutable std::mutex m_mapMutex;
    std::unique_ptr<Pool> m_pool;
};

} // namespace entwine


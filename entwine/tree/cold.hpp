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
#include <entwine/types/structure.hpp>

namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class Chunk;
class ChunkInfo;
class Climber;
class Clipper;
class Entry;
class Pool;
class Schema;

class Cold
{
public:
    Cold(
            arbiter::Endpoint& endpoint,
            const Schema& schema,
            const Structure& structure,
            const std::vector<char>& empty);

    Cold(
            arbiter::Endpoint& endpoint,
            const Schema& schema,
            const Structure& structure,
            const std::vector<char>& empty,
            const Json::Value& meta);

    ~Cold();

    Entry* getEntry(const Climber& climber, Clipper* clipper);

    Json::Value toJson() const;
    void clip(
            const Id& chunkId,
            std::size_t chunkNum,
            Clipper* clipper,
            Pool& pool);

private:
    void growFast(const Climber& climber, Clipper* clipper);
    void growSlow(const Climber& climber, Clipper* clipper);

    struct CountedChunk
    {
        std::unique_ptr<Chunk> chunk;
        std::unordered_set<const Clipper*> refs;
        std::mutex mutex;
    };

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
    const Schema& m_schema;
    const Structure& m_structure;

    std::vector<FastSlot> m_chunkVec;

    ChunkMap m_chunkMap;
    mutable std::mutex m_mapMutex;

    const std::vector<char>& m_empty;
};

} // namespace entwine


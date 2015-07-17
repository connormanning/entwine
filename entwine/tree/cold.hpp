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
#include <unordered_map>

#include <entwine/third/json/json.h>

namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class Chunk;
class ChunkInfo;
class Clipper;
class Entry;
class Pool;
class Schema;
class Structure;

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

    Entry* getEntry(std::size_t index, Clipper* clipper);

    Json::Value toJson() const;
    void clip(std::size_t chunkId, Clipper* clipper, Pool& pool);

private:
    void growFast(const ChunkInfo& info, Clipper* clipper);
    void growSlow(const ChunkInfo& info, Clipper* clipper);

    struct CountedChunk
    {
        std::unique_ptr<Chunk> chunk;
        std::set<const Clipper*> refs;
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

    typedef
        std::unordered_map<std::size_t, std::unique_ptr<CountedChunk>> ChunkMap;

    arbiter::Endpoint& m_endpoint;
    const Schema& m_schema;
    const Structure& m_structure;

    std::vector<FastSlot> m_chunkVec;

    ChunkMap m_chunkMap;
    mutable std::mutex m_mapMutex;

    const std::vector<char>& m_empty;
};

} // namespace entwine


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
#include <condition_variable>
#include <cstddef>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>

#include <entwine/types/structure.hpp>
#include <entwine/third/arbiter/arbiter.hpp>

namespace entwine
{

class Cache;
class ChunkReader;
class Schema;

struct FetchInfo
{
    FetchInfo(
            arbiter::Endpoint& endpoint,
            const Schema& schema,
            const Id& id,
            std::size_t numPoints);

    arbiter::Endpoint& endpoint;
    const Schema& schema;
    Id id;
    std::size_t numPoints;
};

inline bool operator<(const FetchInfo& lhs, const FetchInfo& rhs)
{
    return
        (lhs.endpoint.root() < rhs.endpoint.root()) ||
        (lhs.endpoint.root() == rhs.endpoint.root() && (lhs.id < rhs.id));
}

typedef std::set<FetchInfo> FetchInfoSet;



struct GlobalChunkInfo
{
    GlobalChunkInfo(const std::string& path, const Id& id)
        : path(path)
        , id(id)
    { }

    std::string path;
    Id id;
};

typedef std::list<GlobalChunkInfo> InactiveList;



struct ChunkState
{
    ChunkState();
    ~ChunkState();

    std::unique_ptr<ChunkReader> chunkReader;
    std::unique_ptr<InactiveList::iterator> inactiveIt;
    std::atomic_size_t refs;

    std::mutex mutex;
};



typedef std::map<Id, std::unique_ptr<ChunkState>> LocalManager;
typedef std::map<std::string, LocalManager> GlobalManager;
typedef std::map<Id, const ChunkReader*> ChunkMap;

class Block
{
    friend class Cache;

public:
    ~Block();
    const ChunkMap& chunkMap() const { return m_chunkMap; }
    std::string path() const { return m_readerPath; }

private:
    Block(
            Cache& cache,
            const std::string& readerPath,
            const FetchInfoSet& fetches);

    void set(const Id& id, const ChunkReader* chunkReader);

    Cache& m_cache;
    std::string m_readerPath;
    ChunkMap m_chunkMap;
};

class Cache
{
    friend class Block;

public:
    Cache(std::size_t maxChunks);

    std::unique_ptr<Block> acquire(
            const std::string& readerPath,
            const FetchInfoSet& fetches);

private:
    void release(const Block& block);

    std::unique_ptr<Block> reserve(
            const std::string& readerPath,
            const FetchInfoSet& fetches);

    bool populate(
            const std::string& readerPath,
            const FetchInfoSet& fetches,
            Block& block);

    const ChunkReader* fetch(
            const std::string& readerPath,
            const FetchInfo& fetchInfo);

    std::size_t m_maxChunks;

    GlobalManager m_chunkManager;
    InactiveList m_inactiveList;

    std::atomic_size_t m_activeCount;

    std::mutex m_mutex;
    std::condition_variable m_cv;
};

} // namespace entwine


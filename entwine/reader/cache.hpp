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

#include <entwine/third/arbiter/arbiter.hpp>

namespace entwine
{

class Cache;
class ChunkReader;
class Schema;

class QueryLimitExceeded : public std::runtime_error
{
public:
    QueryLimitExceeded() : std::runtime_error("Query size limit exceeded") { }
};

class InvalidQuery : public std::runtime_error
{
public:
    InvalidQuery()
        : std::runtime_error("Invalid query")
    { }

    InvalidQuery(std::string what)
        : std::runtime_error("Invalid query - " + what)
    { }
};

struct FetchInfo
{
    FetchInfo(
            arbiter::Endpoint& endpoint,
            const Schema& schema,
            std::size_t id,
            std::size_t numPoints);

    arbiter::Endpoint& endpoint;
    const Schema& schema;
    std::size_t id;
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
    GlobalChunkInfo(const std::string& path, std::size_t id)
        : path(path)
        , id(id)
    { }

    std::string path;
    std::size_t id;
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



typedef std::map<std::size_t, std::unique_ptr<ChunkState>> LocalManager;
typedef std::map<std::string, LocalManager> GlobalManager;

typedef std::map<std::size_t, const ChunkReader*> ChunkMap;

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

    void set(std::size_t id, const ChunkReader* chunkReader);

    Cache& m_cache;
    std::string m_readerPath;
    ChunkMap m_chunkMap;
};

class Cache
{
    friend class Block;

public:
    // maxChunks must be at least 16.
    // maxChunksPerQuery must be at least 4.
    Cache(std::size_t maxChunks, std::size_t maxChunksPerQuery);

    std::unique_ptr<Block> acquire(
            const std::string& readerPath,
            const FetchInfoSet& fetches);

    std::size_t queryLimit() const { return m_maxChunksPerQuery; }

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
    std::size_t m_maxChunksPerQuery;

    GlobalManager m_chunkManager;
    InactiveList m_inactiveList;

    std::atomic_size_t m_activeCount;

    std::mutex m_mutex;
    std::condition_variable m_cv;
};

} // namespace entwine


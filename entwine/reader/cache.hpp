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

#include <entwine/reader/hierarchy-reader.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/third/arbiter/arbiter.hpp>

namespace entwine
{

class Cache;
class ChunkReader;
class Reader;
class Schema;

struct FetchInfo
{
    FetchInfo(
            const Reader& reader,
            const Id& id,
            std::size_t depth);

    const Reader& reader;
    const Id id;
    const std::size_t depth;

    bool operator<(const FetchInfo& rhs) const;
};

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



struct DataChunkState
{
    DataChunkState();
    ~DataChunkState();

    std::unique_ptr<ChunkReader> chunkReader;
    std::unique_ptr<InactiveList::iterator> inactiveIt;
    std::atomic_size_t refs;

    std::mutex mutex;
};

using SlotOrder = std::list<const HierarchyReader::Slot*>;
using SlotMap = std::map<const HierarchyReader::Slot*, SlotOrder::iterator>;

struct HierarchyCache
{
    std::mutex mutex;
    SlotMap slots;
    SlotOrder order;
    std::map<const HierarchyReader::Slot*, std::size_t> refs;
};



typedef std::map<Id, std::unique_ptr<DataChunkState>> LocalManager;
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
    Cache(std::size_t maxBytes);

    std::unique_ptr<Block> acquire(
            const std::string& readerPath,
            const FetchInfoSet& fetches);

    void refHierarchySlot(
            const std::string& name,
            const HierarchyReader::Slot* slot);

    void unrefHierarchy(
            const std::string& name,
            const HierarchyReader::Slots& slots);

    std::size_t maxBytes() const { return m_maxBytes; }
    std::size_t activeBytes() const { return m_activeBytes; }

private:
    void release(const Block& block);

    std::unique_ptr<Block> reserve(
            const std::string& readerPath,
            const FetchInfoSet& fetches);

    const ChunkReader* fetch(
            const std::string& readerPath,
            const FetchInfo& fetchInfo);

    const std::size_t m_maxBytes;
    const std::size_t m_maxHierarchyBytes;
    std::size_t m_activeBytes = 0;
    std::size_t m_hierarchyBytes = 0;

    GlobalManager m_chunkManager;
    InactiveList m_inactiveList;

    std::map<std::string, HierarchyCache> m_hierarchyCache;
    std::mutex m_hierarchyMutex;

    std::mutex m_mutex;
    std::condition_variable m_cv;
};

} // namespace entwine


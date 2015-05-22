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
#include <map>
#include <memory>
#include <mutex>
#include <set>

#include <unordered_map>

#include <entwine/third/json/json.h>

namespace entwine
{

class Chunk;
class Clipper;
class Entry;
class Pool;
class Schema;
class Source;
class Structure;

class Cold
{
public:
    Cold(
            Source& source,
            const Schema& schema,
            const Structure& structure,
            const std::vector<char>& empty);

    Cold(
            Source& source,
            const Schema& schema,
            const Structure& structure,
            const std::vector<char>& empty,
            const Json::Value& meta);

    ~Cold();

    Entry* getEntry(std::size_t index, Clipper* clipper);

    Json::Value toJson() const;
    void clip(std::size_t chunkId, Clipper* clipper, Pool& pool);

private:
    std::size_t getChunkId(std::size_t index) const;

    void grow(std::size_t chunkId, Clipper* clipper);

    struct ChunkInfo
    {
        std::unique_ptr<Chunk> chunk;
        std::set<const Clipper*> refs;
        std::mutex mutex;
    };

    typedef
        std::unordered_map<std::size_t, std::unique_ptr<ChunkInfo>> ChunkMap;

    Source& m_source;
    const Schema& m_schema;
    const Structure& m_structure;

    mutable std::mutex m_mutex;
    ChunkMap m_chunks;

    const std::vector<char>& m_empty;
};

} // namespace entwine


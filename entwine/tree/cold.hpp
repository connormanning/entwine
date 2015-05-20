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

#include <entwine/third/json/json.h>

namespace entwine
{

class Chunk;
class Clipper;
class Entry;
class Schema;
class Source;
class Structure;

class Cold
{
public:
    Cold(
            Source& source,
            const Schema& schema,
            const Structure& structure);

    Cold(
            Source& source,
            const Schema& schema,
            const Structure& structure,
            const Json::Value& meta);

    ~Cold();

    Entry* getEntry(std::size_t index, Clipper* clipper);

    Json::Value toJson() const;
    void clip(std::size_t chunkId, Clipper* clipper);

private:
    std::size_t getChunkId(std::size_t index) const;

    void grow(std::size_t chunkId, Clipper* clipper);

    struct ChunkInfo
    {
        std::unique_ptr<Chunk> chunk;
        std::set<const Clipper*> refs;
        std::mutex mutex;
    };

    Source& m_source;
    const Schema& m_schema;
    const Structure& m_structure;

    mutable std::mutex m_mutex;
    std::set<std::size_t> m_ids;
    std::map<std::size_t, ChunkInfo> m_chunks;
};

} // namespace entwine


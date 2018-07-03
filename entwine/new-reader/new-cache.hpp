/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>
#include <deque>
#include <list>
#include <map>
#include <mutex>

#include <entwine/new-reader/new-chunk-reader.hpp>
#include <entwine/types/key.hpp>

namespace entwine
{

class NewReader;

struct GlobalId
{
    GlobalId(const std::string path, const Dxyz& key)
        : path(path)
        , key(key)
    { }

    const std::string path;
    const Dxyz key;
};

bool operator<(const GlobalId& a, const GlobalId& b);

struct ChunkReaderInfo
{
    using Map = std::map<GlobalId, ChunkReaderInfo>;
    using Order = std::list<Map::iterator>;

    SharedChunkReader chunk;
    Order::iterator it;
};

class NewCache
{
public:
    NewCache(std::size_t maxBytes = 1024 * 1024 * 1024) // TODO default.
        : m_maxBytes(maxBytes)
    { }

    std::size_t maxBytes() const { return m_maxBytes; }

    std::deque<SharedChunkReader> acquire(
            const NewReader& reader,
            const std::vector<Dxyz>& keys);

private:
    SharedChunkReader get(const NewReader& reader, const Dxyz& id);
    void purge();

    const std::size_t m_maxBytes = 1024 * 1024;

    mutable std::mutex m_mutex;
    std::size_t m_size = 0;

    ChunkReaderInfo::Map m_chunks;
    ChunkReaderInfo::Order m_order;
};

} // namespace entwine


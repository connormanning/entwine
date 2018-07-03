/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/new-reader/new-cache.hpp>

#include <entwine/new-reader/new-reader.hpp>

namespace entwine
{

bool operator<(const GlobalId& a, const GlobalId& b)
{
    return a.path < b.path || (a.path == b.path && a.key < b.key);
}

std::deque<SharedChunkReader> NewCache::acquire(
        const NewReader& reader,
        const std::vector<Dxyz>& keys)
{
    std::deque<SharedChunkReader> block;

    std::lock_guard<std::mutex> lock(m_mutex);
    for (const Dxyz& key : keys) block.push_back(get(reader, key));

    purge();

    return block;
}

SharedChunkReader NewCache::get(const NewReader& reader, const Dxyz& key)
{
    const GlobalId id(reader.path(), key);

    auto it(m_chunks.find(id));

    if (it == m_chunks.end())
    {
        it = m_chunks.insert(std::make_pair(id, ChunkReaderInfo())).first;

        ChunkReaderInfo& info(it->second);
        info.chunk = std::make_shared<NewChunkReader>(reader, key);
        m_size += info.chunk->cells().size() * reader.pointSize();
    }
    else
    {
        ChunkReaderInfo& info(it->second);
        m_order.erase(info.it);
    }

    m_order.push_front(it);

    ChunkReaderInfo& info(it->second);
    info.it = m_order.begin();

    return info.chunk;
}

void NewCache::purge()
{
    const std::size_t start(m_size);
    while (m_size > m_maxBytes)
    {
        const auto& it(m_order.back());
        const GlobalId& id(it->first);
        const ChunkReaderInfo& info(it->second);

        std::cout << "\tDele " << id.key << std::endl;
        m_size -= info.chunk->cells().size() * info.chunk->pointSize();
        m_order.pop_back();
        m_chunks.erase(it);
    }

    if (start > m_size)
    {
        std::cout << "\t\tPurged " << (start - m_size) << std::endl;
    }
}

} // namespace entwine


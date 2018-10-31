/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/reader/cache.hpp>

#include <entwine/reader/reader.hpp>

namespace entwine
{

bool operator<(const GlobalId& a, const GlobalId& b)
{
    return a.path < b.path || (a.path == b.path && a.key < b.key);
}

std::deque<SharedChunkReader> Cache::acquire(
        const Reader& reader,
        const std::vector<Dxyz>& keys)
{
    std::deque<SharedChunkReader> block;

    std::lock_guard<std::mutex> lock(m_mutex);
    for (const Dxyz& key : keys) block.push_back(get(reader, key));

    purge();

    return block;
}

SharedChunkReader Cache::get(const Reader& reader, const Dxyz& key)
{
    const GlobalId id(reader.path(), key);

    auto it(m_chunks.find(id));

    if (it == m_chunks.end())
    {
        // std::cout << "\tAdd " << id.key << std::endl;
        it = m_chunks.insert(std::make_pair(id, ChunkReaderInfo())).first;

        ChunkReaderInfo& info(it->second);
        info.chunk = std::make_shared<ChunkReader>(reader, key);
        m_size += info.chunk->bytes();
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

void Cache::purge()
{
    const std::size_t start(m_size);
    while (m_size > m_maxBytes)
    {
        const auto& it(m_order.back());
        const GlobalId& id(it->first);
        const ChunkReaderInfo& info(it->second);

        std::cout << "\tDel " << id.key << std::endl;
        m_size -= info.chunk->bytes();
        m_order.pop_back();
        m_chunks.erase(it);
    }

    if (start > m_size)
    {
        std::cout << "\t\tPurged " << (start - m_size) << std::endl;
        std::cout << "\t\tLeft " << m_size << std::endl;
    }
}

} // namespace entwine


/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <entwine/types/key.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/types/voxel.hpp>

namespace entwine
{

class Overflow
{
    struct Entry
    {
        Entry(const Key& key) : key(key) { }
        Voxel voxel;
        Key key;
    };

public:
    Overflow(const ChunkKey& ck)
        : m_chunkKey(ck)
        , m_pointSize(ck.metadata().schema().pointSize())
        , m_block(m_pointSize, 256)
    { }

    bool insert(Voxel& voxel, Key& key)
    {
        Entry entry(key);
        entry.voxel.setData(m_block.next());
        entry.voxel.initDeep(voxel.point(), voxel.data(), m_pointSize);
        m_list.push_back(entry);
        return true;
    }

    const ChunkKey& chunkKey() const { return m_chunkKey; }
    MemBlock& block() { return m_block; }
    uint64_t size() const { return m_block.size(); }
    std::vector<Entry>& list() { return m_list; }

private:
    const ChunkKey m_chunkKey;
    const uint64_t m_pointSize = 0;

    MemBlock m_block;
    std::vector<Entry> m_list;
};

} // namespace entwine


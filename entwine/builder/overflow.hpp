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

struct Overflow
{
    struct Entry
    {
        Entry(const Key& key) : key(key) { }
        Voxel voxel;
        Key key;
    };

    Overflow(const ChunkKey& chunkKey, uint64_t pointSize)
        : chunkKey(chunkKey)
        , pointSize(pointSize)
        , block(pointSize, 256)
    { }

    void insert(Voxel& voxel, Key& key)
    {
        Entry entry(key);
        entry.voxel.setData(block.next());
        entry.voxel.initDeep(voxel.point(), voxel.data(), pointSize);
        list.push_back(entry);
    }

    const ChunkKey chunkKey;
    const uint64_t pointSize = 0;

    MemBlock block;
    std::vector<Entry> list;
};

} // namespace entwine


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

#include <map>

#include <entwine/new-reader/new-chunk-reader.hpp>
#include <entwine/types/key.hpp>

namespace entwine
{

struct GlobalKey
{
    GlobalKey(std::string name, const Dxyz& id) : name(name), id(id) { }

    const std::string name;
    const Dxyz id;
};

class NewCache
{
    using ChunkMap = std::map<GlobalKey, NewChunkReader>;

public:
    std::size_t maxBytes() const { return 0; }

private:
     ChunkMap m_chunks;
};

} // namespace entwine


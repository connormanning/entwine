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

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <json/json.h>

#include <entwine/types/defs.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/format-types.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace arbiter { class Endpoint; }
class Chunk;
class ChunkStorage;
class Metadata;

// The Format contains the attributes that give insight about what the tree
// looks like at a more micro-oriented level than the Structure, which gives
// information about the overall tree structure.  Whereas the Structure can
// tell us about the chunks that exist in the tree, the Format can tell us
// about what those chunks look like.
class Format
{
public:
    Format(
            const Metadata& metadata,
            ChunkCompression compression = ChunkCompression::LasZip,
            HierarchyCompression hc = HierarchyCompression::Lzma);
    Format(const Metadata& metadata, const Format& other);
    Format(const Metadata& metadata, const Json::Value& json);
    Format(const Format&) = delete;
    ~Format();

    Json::Value toJson() const;

    void serialize(Chunk& chunk) const;
    Cell::PooledStack deserialize(
        const arbiter::Endpoint& endpoint,
        PointPool& pool,
        const Id& chunkId) const;

    ChunkCompression compression() const { return m_compression; }
    HierarchyCompression hierarchyCompression() const
    {
        return m_hierarchyCompression;
    }

    const Metadata& metadata() const;
    const Schema& schema() const;

private:
    const Metadata& m_metadata;
    const Json::Value m_json;

    ChunkCompression m_compression;
    HierarchyCompression m_hierarchyCompression;

    std::unique_ptr<ChunkStorage> m_storage;
};

} // namespace entwine


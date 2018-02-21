/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

/*
#pragma once

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <json/json.h>

#include <entwine/types/defs.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/storage-types.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace arbiter { class Endpoint; }

class Chunk;
class ChunkStorage;
class Metadata;

class Storage
{
public:
    Storage(const Metadata& metadata);
    Storage(
            const Metadata& metadata,
            ChunkStorageType compression = ChunkStorageType::LasZip,
            HierarchyCompression hc = HierarchyCompression::Lzma);
    Storage(const Metadata& metadata, const Storage& other);
    Storage(const Metadata& metadata, const Json::Value& json);
    Storage(const Storage&) = delete;
    ~Storage();

    Json::Value toJson() const;

    void write(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pointPool,
            const std::string& filename,
            const Bounds& bounds,
            Cell::PooledStack&& cells) const;

    Cell::PooledStack read(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pointPool,
            const std::string& filename) const;

    const Metadata& metadata() const;
    const Schema& schema() const;

private:
    const Metadata& m_metadata;
    const Json::Value m_json;

    ChunkStorageType m_chunkStorageType;
    HierarchyCompression m_hierarchyCompression;

    std::unique_ptr<ChunkStorage> m_storage;
    ChunkStorage m_storage
};

} // namespace entwine
*/


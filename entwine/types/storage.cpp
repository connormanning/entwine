/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/storage.hpp>

#include <numeric>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/types/binary-point-table.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/util/compression.hpp>
#include <entwine/util/unique.hpp>

#include <entwine/types/chunk-storage/chunk-storage.hpp>

namespace entwine
{

Storage::Storage(
        const Metadata& metadata,
        const ChunkStorageType chunkStorageType,
        const HierarchyCompression hierarchyCompression)
    : m_metadata(metadata)
    , m_chunkStorageType(chunkStorageType)
    , m_hierarchyCompression(hierarchyCompression)
{
    m_storage = ChunkStorage::create(m_metadata, m_chunkStorageType);
}

Storage::Storage(const Metadata& metadata, const Json::Value& json)
    : m_metadata(metadata)
    , m_json(json)
    , m_chunkStorageType(toChunkStorageType(json["storage"]))
    , m_hierarchyCompression(toHierarchyCompression(json["compressHierarchy"]))
{
    m_storage = ChunkStorage::create(m_metadata, m_chunkStorageType, m_json);
}

Storage::Storage(const Metadata& metadata, const Storage& other)
    : m_metadata(metadata)
    , m_json(other.m_json)
    , m_chunkStorageType(other.m_chunkStorageType)
    , m_hierarchyCompression(other.m_hierarchyCompression)
{
    m_storage = ChunkStorage::create(m_metadata, m_chunkStorageType, m_json);
}

Storage::~Storage() { }

Json::Value Storage::toJson() const
{
    Json::Value json;
    json["storage"] = toString(m_chunkStorageType);
    json["compressHierarchy"] = toString(m_hierarchyCompression);

    const auto s(m_storage->toJson());
    for (const auto f : s.getMemberNames()) json[f] = s[f];

    return json;
}

void Storage::serialize(Chunk& chunk) const
{
    if (m_metadata.cesiumSettings()) chunk.tile();
    m_storage->write(chunk);
}

Cell::PooledStack Storage::deserialize(
        const arbiter::Endpoint& endpoint,
        PointPool& pool,
        const Id& chunkId) const
{
    return m_storage->read(endpoint, pool, chunkId);
}

const Metadata& Storage::metadata() const { return m_metadata; }
const Schema& Storage::schema() const { return m_metadata.schema(); }

} // namespace entwine


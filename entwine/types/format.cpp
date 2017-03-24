/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/format.hpp>

#include <numeric>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/builder.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/types/binary-point-table.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/util/compression.hpp>
#include <entwine/util/storage.hpp>
#include <entwine/util/unique.hpp>

#include <entwine/tree/storage/storage.hpp>
#include <entwine/tree/storage/lazperf.hpp>
#include <entwine/tree/storage/laszip.hpp>

namespace entwine
{

Format::Format(
        const Metadata& metadata,
        const bool trustHeaders,
        const ChunkCompression compression,
        const HierarchyCompression hierarchyCompression)
    : m_metadata(metadata)
    , m_trustHeaders(trustHeaders)
    , m_compression(compression)
    , m_hierarchyCompression(hierarchyCompression)
{
    m_storage = ChunkStorage::create(m_metadata, m_compression);
}

Format::Format(const Metadata& metadata, const Json::Value& json)
    : m_metadata(metadata)
    , m_json(json)
    , m_trustHeaders(json["trustHeaders"].asBool())
    , m_compression(toCompression(json["compression"]))
    , m_hierarchyCompression(toHierarchyCompression(json["compressHierarchy"]))
{
    m_storage = ChunkStorage::create(m_metadata, m_compression, m_json);
}

Format::Format(const Metadata& metadata, const Format& other)
    : m_metadata(metadata)
    , m_json(other.m_json)
    , m_trustHeaders(other.m_trustHeaders)
    , m_compression(other.m_compression)
    , m_hierarchyCompression(other.m_hierarchyCompression)
{
    m_storage = ChunkStorage::create(m_metadata, m_compression, m_json);
}

Format::~Format() { }

Json::Value Format::toJson() const
{
    Json::Value json;
    json["trustHeaders"] = m_trustHeaders;
    json["compression"] = toString(m_compression);
    json["compressHierarchy"] = toString(m_hierarchyCompression);

    const auto s(m_storage->toJson());
    for (const auto f : s.getMemberNames()) json[f] = s[f];

    return json;
}

void Format::serialize(Chunk& chunk) const
{
    if (m_metadata.cesiumSettings()) chunk.tile();
    m_storage->write(chunk);
}

Cell::PooledStack Format::deserialize(
        const arbiter::Endpoint& endpoint,
        PointPool& pool,
        const Id& chunkId) const
{
    return m_storage->read(endpoint, pool, chunkId);
}

const Metadata& Format::metadata() const { return m_metadata; }
const Schema& Format::schema() const { return m_metadata.schema(); }

} // namespace entwine


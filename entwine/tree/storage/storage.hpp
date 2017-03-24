/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <json/json.h>

#include <entwine/tree/builder.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/types/format-types.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/util/storage.hpp>

namespace entwine
{

class ChunkStorage
{
public:
    ChunkStorage(const Metadata& metadata) : m_metadata(metadata) { }
    virtual ~ChunkStorage() { }

    static std::unique_ptr<ChunkStorage> create(
            const Metadata& metadata,
            ChunkCompression compression,
            const Json::Value& json = Json::nullValue);

    virtual void write(Chunk& chunk) const = 0;
    virtual Cell::PooledStack read(
            const arbiter::Endpoint& endpoint,
            PointPool& pool,
            const Id& id) const = 0;

    virtual Json::Value toJson() const { return Json::nullValue; }

protected:
    void ensurePut(
            const Chunk& chunk,
            const std::string& path,
            const std::vector<char>& data) const
    {
        Storage::ensurePut(chunk.builder().outEndpoint(), path, data);
    }

    std::unique_ptr<std::vector<char>> ensureGet(
            const Chunk& chunk,
            const std::string& path) const
    {
        return Storage::ensureGet(chunk.builder().outEndpoint(), path);
    }

    const Metadata& m_metadata;
};

} // namespace entwine


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

#include <string>

#include <json/json.h>

#include <entwine/types/metadata.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/storage-types.hpp>
#include <entwine/util/io.hpp>

namespace entwine
{

class ChunkStorage
{
public:
    ChunkStorage(const Metadata& metadata) : m_metadata(metadata) { }
    virtual ~ChunkStorage() { }

    static std::unique_ptr<ChunkStorage> create(
            const Metadata& m,
            std::string type);

    virtual void write(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pointPool,
            const std::string& filename,
            Cell::PooledStack&& cells) const
    {
        throw std::runtime_error("ChunkStorage::write not implemented");
    }

    virtual Cell::PooledStack read(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pointPool,
            const std::string& filename) const
    {
        throw std::runtime_error("ChunkStorage::read not implemented");
    }

    virtual Json::Value toJson() const { return Json::nullValue; }

protected:
    /*
    void ensurePut(
            const Chunk& chunk,
            const std::string& path,
            const std::vector<char>& data) const
    {
        io::ensurePut(chunk.builder().outEndpoint(), path, data);
    }

    std::unique_ptr<std::vector<char>> ensureGet(
            const Chunk& chunk,
            const std::string& path) const
    {
        return io::ensureGet(chunk.builder().outEndpoint(), path);
    }
    */

    const Metadata& m_metadata;
};

} // namespace entwine


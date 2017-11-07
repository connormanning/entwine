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

#include <entwine/types/chunk-storage/chunk-storage.hpp>

namespace entwine
{

class LasZipStorage : public ChunkStorage
{
public:
    LasZipStorage(const Metadata& m, const Json::Value& json = Json::nullValue)
        : ChunkStorage(m)
    { }

    virtual void write(Chunk& chunk) const override;

    virtual Cell::PooledStack read(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pool,
            const Id& id) const override;

    virtual std::string filename(const Id& id) const override
    {
        return m_metadata.basename(id) + ".laz";
    }
};

} // namespace entwine


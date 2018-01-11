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

#include <entwine/types/chunk-storage/binary.hpp>
#include <entwine/util/compression.hpp>

namespace entwine
{

class LazPerfStorage : public BinaryStorage
{
public:
    LazPerfStorage(const Metadata& m, const Json::Value& json = Json::nullValue)
        : BinaryStorage(m, json)
    { }

    virtual void write(Chunk& chunk) const override;

    virtual Cell::PooledStack read(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pool,
            const Id& id) const override;
};

} // namespace entwine


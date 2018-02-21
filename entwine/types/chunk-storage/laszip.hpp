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
    LasZipStorage(const Metadata& m) : ChunkStorage(m) { }

    virtual void write(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pointPool,
            const std::string& filename,
            const Bounds& bounds,
            Cell::PooledStack&& cells) const override;

    virtual Cell::PooledStack read(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pool,
            const std::string& filename) const override;
};

} // namespace entwine


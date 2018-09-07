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

#include <memory>

#include <entwine/types/key.hpp>

namespace entwine
{

class Reader;

class ChunkReader
{
public:
    ChunkReader(const Reader& reader, const Dxyz& id);
    ~ChunkReader()
    {
        m_pointPool.release(std::move(m_cells));
    }

    const Cell::PooledStack& cells() const { return m_cells; }
    uint64_t pointSize() const { return m_pointSize; }

private:
    PointPool m_pointPool;
    Cell::PooledStack m_cells;
    uint64_t m_pointSize;
};

using SharedChunkReader = std::shared_ptr<ChunkReader>;

} // namespace entwine


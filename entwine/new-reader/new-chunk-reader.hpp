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
#include <entwine/types/point-pool.hpp>

namespace entwine
{

class NewReader;

class NewChunkReader
{
public:
    NewChunkReader(const NewReader& reader, const Dxyz& id);
    ~NewChunkReader()
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

using SharedChunkReader = std::shared_ptr<NewChunkReader>;

} // namespace entwine


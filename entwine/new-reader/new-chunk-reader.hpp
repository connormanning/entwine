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

#include <entwine/types/key.hpp>
#include <entwine/types/point-pool.hpp>

namespace entwine
{

class NewReader;

class NewChunkReader
{
public:
    NewChunkReader(const NewReader& reader, const Dxyz& id);

    const Cell::PooledStack& cells() const { return m_cells; }

private:
    Cell::PooledStack m_cells;
};

} // namespace entwine


/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/io/binary.hpp>

#include <pdal/io/LasWriter.hpp>

#include <entwine/util/executor.hpp>

namespace entwine
{

void Binary::write(
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        PointPool& pointPool,
        const std::string& filename,
        Cell::PooledStack&& cells) const
{
}

Cell::PooledStack Binary::read(
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        PointPool& pool,
        const std::string& filename) const
{
    return Cell::PooledStack(pool.cellPool());
}

} // namespace entwine


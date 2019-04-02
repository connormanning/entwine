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
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class Reader;

class ChunkReader
{
public:
    ChunkReader(const Reader& reader, const Dxyz& id);

    VectorPointTable& table() { return *m_table; }
    std::size_t bytes() const
    {
        return m_table->capacity() * m_table->pointSize();
    }

private:
    std::unique_ptr<VectorPointTable> m_table;
};

using SharedChunkReader = std::shared_ptr<ChunkReader>;

} // namespace entwine


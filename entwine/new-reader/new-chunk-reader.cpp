/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/new-reader/new-chunk-reader.hpp>

#include <entwine/io/io.hpp>
#include <entwine/new-reader/new-reader.hpp>

namespace entwine
{

NewChunkReader::NewChunkReader(const NewReader& r, const Dxyz& id)
    : m_pointPool(r.metadata().schema(), r.metadata().delta(), 4096)
    , m_cells(r.metadata().dataIo().read(
                r.ep(),
                r.tmp(),
                m_pointPool,
                id.toString()))
    , m_pointSize(r.metadata().schema().pointSize())
{ }

} // namespace entwine


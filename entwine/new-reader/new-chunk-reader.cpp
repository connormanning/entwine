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

#include <entwine/types/chunk-storage/chunk-storage.hpp>
#include <entwine/new-reader/new-reader.hpp>

namespace entwine
{

NewChunkReader::NewChunkReader(const NewReader& r, const Dxyz& id)
    : m_cells(r.metadata().storage().read(
                r.ep(),
                r.tmp(),
                r.pointPool(),
                id.toString()))
{ }

} // namespace entwine


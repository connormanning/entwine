/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/reader/chunk-reader.hpp>

#include <entwine/io/io.hpp>
#include <entwine/reader/reader.hpp>

namespace entwine
{

ChunkReader::ChunkReader(const Reader& r, const Dxyz& id)
{
    std::vector<char> data;

    VectorPointTable tmp(r.metadata().schema());
    tmp.setProcess([&data, &tmp]()
    {
        data.insert(
                data.end(),
                tmp.data().data(),
                tmp.data().data() + tmp.numPoints() * tmp.pointSize());
    });

    r.metadata().dataIo().read(r.ep(), r.tmp(), id.toString(), tmp);

    m_table = makeUnique<VectorPointTable>(
            r.metadata().schema(),
            std::move(data));
    m_table->clear(m_table->capacity());
}

} // namespace entwine


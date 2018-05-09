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

#include <cstddef>

#include <entwine/types/defs.hpp>
#include <entwine/types/key.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class NewClimber
{
public:
    NewClimber(const Metadata& metadata, Origin origin = 0)
        : m_metadata(metadata)
        , m_structure(m_metadata.structure())
        , m_origin(origin)
        , m_point(m_metadata)
        , m_chunk(m_metadata)
    { }

    void reset()
    {
        m_point.reset();
        m_chunk.reset();
    }

    void init(const Point& p)
    {
        init(p, m_structure.head());
    }

    void init(const Point& p, uint64_t depth)
    {
        reset();
        while (m_chunk.d < depth) step(p);
    }

    void step(const Point& p)
    {
        m_point.step(p);
        m_chunk.step(p);
    }

    Origin origin() const { return m_origin; }
    uint64_t depth() const { return m_chunk.d; }
    const Key& pointKey() const { return m_point; }
    const Key& chunkKey() const { return m_chunk.k; }
    std::size_t pointSize() const { return m_metadata.schema().pointSize(); }

private:
    const Metadata& m_metadata;
    const NewStructure& m_structure;
    const Origin m_origin;

    Key m_point;
    ChunkKey m_chunk;
};

} // namespace entwine


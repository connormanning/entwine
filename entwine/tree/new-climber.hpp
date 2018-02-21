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

#include <entwine/tree/key.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point.hpp>
#include <entwine/types/structure.hpp>

namespace entwine
{

class NewClimber
{
public:
    NewClimber(const Metadata& metadata, Origin origin)
        : m_metadata(metadata)
        , m_structure(m_metadata.structure())
        , m_origin(origin)
        , m_point(m_metadata)
        , m_chunk(m_metadata)
    { }

    void reset()
    {
        m_d = 0;
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
        while (m_d < depth) step(p);
    }

    void step(const Point& p)
    {
        m_point.step(p);
        if (m_d >= m_structure.body() && m_d < m_structure.tail())
        {
            m_chunk.step(p);
        }

        ++m_d;
    }

    Origin origin() const { return m_origin; }
    uint64_t depth() const { return m_d; }
    const Key& pointKey() const { return m_point; }
    const Key& chunkKey() const { return m_chunk; }
    std::size_t pointSize() const { return m_metadata.schema().pointSize(); }

private:
    const Metadata& m_metadata;
    const NewStructure& m_structure;
    const Origin m_origin;

    uint64_t m_d = 0;

    Key m_point;
    Key m_chunk;
};

} // namespace entwine


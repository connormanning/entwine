/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <pdal/PointTable.hpp>

#include <entwine/types/schema.hpp>

namespace entwine
{

class BinaryPointTable : public pdal::StreamPointTable
{
public:
    BinaryPointTable(const Schema& schema)
        : pdal::StreamPointTable(schema.pdalLayout())
        , m_pos(nullptr)
    { }

    BinaryPointTable(const Schema& schema, const char* pos)
        : pdal::StreamPointTable(schema.pdalLayout())
        , m_pos(pos)
    { }

    virtual pdal::point_count_t capacity() const override { return 1; }
    virtual char* getPoint(pdal::PointId i) override
    {
        // :(
        return const_cast<char*>(m_pos);
    }

    void setPoint(const char* pos) { m_pos = pos; }

protected:
    const char* m_pos;
};

} // namespace entwine


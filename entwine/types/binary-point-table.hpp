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
#include <pdal/PointRef.hpp>

#include <entwine/types/schema.hpp>

namespace entwine
{

class BinaryPointTable : public pdal::StreamPointTable
{
public:
    BinaryPointTable(const Schema& schema)
        : pdal::StreamPointTable(schema.pdalLayout(), 1)
        , m_pos(nullptr)
        , m_ref(*this, 0)
    { }

    BinaryPointTable(const Schema& schema, const char* pos)
        : pdal::StreamPointTable(schema.pdalLayout(), 1)
        , m_pos(pos)
        , m_ref(*this, 0)
    { }

    virtual char* getPoint(pdal::PointId i) override
    {
        // :(
        return const_cast<char*>(m_pos);
    }

    void setPoint(const char* pos) { m_pos = pos; }
    pdal::PointRef& ref() { return m_ref; }
    const pdal::PointRef& ref() const { return m_ref; }

protected:
    const char* m_pos;
    pdal::PointRef m_ref;
};

} // namespace entwine


/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>
#include <string>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/io.hpp>

namespace entwine
{

class Append
{
public:
    Append(
            const arbiter::Endpoint& ep,
            std::string name,
            const Schema& schema,
            const Id& id,
            std::size_t numPoints)
        : m_ep(ep)
        , m_filename("d/" + name + "/" + id.str())
        , m_schema(schema.filter("Skip"))
        , m_dimTypeList(m_schema.pdalLayout().dimTypes())
        , m_table(m_schema, numPoints)
    {
        if (m_ep.tryGetSize(m_filename))
        {
            m_table.data() = m_ep.getBinary(m_filename);
        }
    }

    void insert(const pdal::PointRef& pr, std::size_t offset)
    {
        m_touched = true;
        pr.getPackedData(m_dimTypeList, m_table.getPoint(offset));
    }

    void write() const
    {
        if (m_touched)
        {
            std::cout << "Writing " << m_filename << std::endl;
            io::ensurePut(m_ep, m_filename, m_table.data());
        }
    }

    const Schema& schema() const { return m_schema; }
    VectorPointTable& table() { return m_table; }

private:
    const arbiter::Endpoint m_ep;
    const std::string m_filename;

    const Schema m_schema;
    const pdal::DimTypeList m_dimTypeList;

    VectorPointTable m_table;
    bool m_touched = false;
};

} // namespace entwine


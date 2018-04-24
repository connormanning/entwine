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

#include <entwine/new-reader/query-params.hpp>

#include <entwine/new-reader/hierarchy-reader.hpp>
#include <entwine/new-reader/filter.hpp>
#include <entwine/types/binary-point-table.hpp>
#include <entwine/types/key.hpp>

namespace entwine
{

class NewReader;

class NewQuery
{
public:
    NewQuery(const NewReader& reader, const NewQueryParams& params);
    virtual ~NewQuery() { }

protected:
    const Metadata& m_metadata;
    const HierarchyReader& m_hierarchy;
    const NewQueryParams m_params;
    const Filter m_filter;

    BinaryPointTable m_table;
    pdal::PointRef m_pointRef;

    HierarchyReader::Keys m_overlaps;

private:
    HierarchyReader::Keys overlaps() const;
    void overlaps(HierarchyReader::Keys& keys, const ChunkKey& c) const;
};

} // namespace entwine


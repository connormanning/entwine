/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/simple-point-layout.hpp>

namespace
{
    bool contains(
            const pdal::Dimension::IdList& idList,
            const pdal::Dimension::Id::Enum id)
    {
        for (const auto current : idList)
        {
            if (current == id) return true;
        }

        return false;
    }
}

bool SimplePointLayout::update(
        pdal::Dimension::Detail dimDetail,
        const std::string& name)
{
    bool added(false);

    if (!m_finalized && !contains(m_used, dimDetail.id()))
    {
        dimDetail.setOffset(m_pointSize);

        m_pointSize += dimDetail.size();
        m_used.push_back(dimDetail.id());
        m_detail[dimDetail.id()] = dimDetail;

        added = true;
    }

    return added;
}


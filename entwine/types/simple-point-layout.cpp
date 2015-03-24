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

#include <pdal/Utils.hpp>

bool SimplePointLayout::update(
        pdal::Dimension::Detail dimDetail,
        const std::string& name)
{
    bool added(false);

    if (!m_finalized && !pdal::Utils::contains(m_used, dimDetail.id()))
    {
        dimDetail.setOffset(m_pointSize);

        m_pointSize += dimDetail.size();
        m_used.push_back(dimDetail.id());
        m_detail[dimDetail.id()] = dimDetail;

        added = true;
    }

    return added;
}


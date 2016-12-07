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

#include <pdal/PointLayout.hpp>
#include <pdal/util/Utils.hpp>

namespace entwine
{

class FixedPointLayout : public pdal::PointLayout
{
private:
    virtual bool update(
            pdal::Dimension::Detail dimDetail,
            const std::string& name) override
    {
        if (!m_finalized)
        {
            if (!contains(m_used, dimDetail.id()))
            {
                dimDetail.setOffset(m_pointSize);

                m_pointSize += dimDetail.size();
                m_used.push_back(dimDetail.id());
                m_detail[pdal::Utils::toNative(dimDetail.id())] = dimDetail;

                return true;
            }
        }
        else return m_propIds.count(name);

        return false;
    }

    bool contains(
            const pdal::Dimension::IdList& idList,
            const pdal::Dimension::Id id) const
    {
        for (const auto current : idList)
        {
            if (current == id) return true;
        }

        return false;
    }
};

} // namespace entwine


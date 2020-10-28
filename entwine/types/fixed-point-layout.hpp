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

#include <string>
#include <vector>

#include <pdal/PointLayout.hpp>
#include <pdal/util/Utils.hpp>

namespace entwine
{

class FixedPointLayout : public pdal::PointLayout
{
public:
    using Added = std::vector<std::string>;
    const Added& added() const { return m_added; }

    void registerFixedDim(pdal::Dimension::Id id, pdal::Dimension::Type type)
    {
        using namespace pdal::Dimension;

        Detail dd = m_detail[pdal::Utils::toNative(id)];
        dd.setType(type);
        update(dd, pdal::Dimension::name(id));
    }

    pdal::Dimension::Id registerOrAssignFixedDim(
        const std::string name,
        pdal::Dimension::Type type)
    {
        pdal::Dimension::Id id = pdal::Dimension::id(name);
        if (id != pdal::Dimension::Id::Unknown)
        {
            registerFixedDim(id, type);
            return id;
        }
        return assignDim(name, type);
    }

private:
    virtual bool update(
            pdal::Dimension::Detail dimDetail,
            const std::string& name) override
    {
        if (std::find(m_added.begin(), m_added.end(), name) == m_added.end())
        {
            m_added.push_back(name);
        }

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

    Added m_added;
};

} // namespace entwine


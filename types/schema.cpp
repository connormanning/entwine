/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "schema.hpp"

namespace
{
    pdal::PointContext getPointContext(std::vector<DimInfo>& dims)
    {
        pdal::PointContext pointContext;
        for (auto& dim : dims)
        {
            dim.setId(
                    pointContext.registerOrAssignDim(dim.name(), dim.type()));
        }
        return pointContext;
    }
}

Schema::Schema(std::vector<DimInfo> dims)
    : m_dims(dims)
    , m_pointContext(getPointContext(dims))
{ }

std::size_t Schema::stride() const
{
    return m_pointContext.pointSize();
}

const std::vector<DimInfo>& Schema::dims() const
{
    return m_dims;
}

pdal::PointContext Schema::pointContext() const
{
    return m_pointContext;
}


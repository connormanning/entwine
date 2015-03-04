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

Json::Value Schema::toJson() const
{
    Json::Value json;
    for (const auto& dim : m_dims)
    {
        Json::Value cur;
        cur["name"] = dim.name();
        cur["type"] = dim.typeString();
        cur["size"] = static_cast<Json::UInt64>(dim.size());
        json.append(cur);
    }
    return json;
}

Schema Schema::fromJson(const Json::Value& json)
{
    std::vector<DimInfo> dims;
    for (Json::ArrayIndex i(0); i < json.size(); ++i)
    {
        const Json::Value& jsonDim(json[i]);
        dims.push_back(
                DimInfo(
                    jsonDim["name"].asString(),
                    jsonDim["type"].asString(),
                    jsonDim["size"].asUInt64()));
    }
    return Schema(dims);
}


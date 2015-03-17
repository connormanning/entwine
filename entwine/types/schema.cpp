/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/schema.hpp>

namespace
{
    pdal::PointLayoutPtr getPointLayout(std::vector<entwine::DimInfo>& dims)
    {
        pdal::PointLayoutPtr layout(new pdal::PointLayout());
        for (auto& dim : dims)
        {
            dim.setId(layout->registerOrAssignDim(dim.name(), dim.type()));
        }
        return layout;
    }

    /*
    std::vector<entwine::DimInfo> getDims(
            const pdal::PointContextRef pointContext)
    {
        std::vector<entwine::DimInfo> dims;
        for (const auto& id : pointContext.dims())
        {
            dims.push_back(
                    entwine::DimInfo(
                        pointContext.dimName(id),
                        id,
                        pointContext.dimType(id)));

        }
        return dims;
    }
    */
}

namespace entwine
{

Schema::Schema(std::vector<DimInfo> dims)
    : m_dims(dims)
    , m_layout(getPointLayout(m_dims))
{ }

/*
Schema::Schema(const pdal::PointContextRef pointContext)
    : m_dims(getDims(pointContext))
    , m_pointContext(getPointContext(m_dims))
{ }
*/

std::size_t Schema::pointSize() const
{
    return m_layout->pointSize();
}

const std::vector<DimInfo>& Schema::dims() const
{
    return m_dims;
}

const pdal::PointLayoutPtr Schema::pdalLayout() const
{
    return m_layout;
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

} // namespace entwine


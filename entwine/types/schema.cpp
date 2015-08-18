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

#include <entwine/types/simple-point-layout.hpp>

namespace
{
    std::unique_ptr<pdal::PointLayout> makePointLayout(
            std::vector<entwine::DimInfo>& dims)
    {
        std::unique_ptr<pdal::PointLayout> layout(new SimplePointLayout());

        for (auto& dim : dims)
        {
            dim.setId(layout->registerOrAssignDim(dim.name(), dim.type()));
        }

        layout->finalize();

        return layout;
    }
}

namespace entwine
{

Schema::Schema()
    : m_layout(new SimplePointLayout())
    , m_dims()
{ }

Schema::Schema(DimList dims)
    : m_layout(makePointLayout(dims))
    , m_dims(dims)
{ }

Schema::Schema(const Json::Value& json)
    : m_layout()
    , m_dims()
{
    for (Json::ArrayIndex i(0); i < json.size(); ++i)
    {
        const Json::Value& jsonDim(json[i]);
        const Json::Value& sizeDim(jsonDim["size"]);
        m_dims.push_back(
                DimInfo(
                    jsonDim["name"].asString(),
                    jsonDim["type"].asString(),
                    sizeDim.isIntegral() ?
                        sizeDim.asUInt64() : std::stoul(sizeDim.asString())));
    }

    m_layout = makePointLayout(m_dims);
}

Schema::Schema(const Schema& other)
{
    m_dims = other.m_dims;
    m_layout = makePointLayout(m_dims);
}

Schema& Schema::operator=(const Schema& other)
{
    m_dims = other.m_dims;
    m_layout = makePointLayout(m_dims);

    return *this;
}

Schema::~Schema()
{ }

void Schema::finalize()
{
    m_layout->finalize();

    for (const auto& id : m_layout->dims())
    {
        m_dims.push_back(
                entwine::DimInfo(
                    m_layout->dimName(id),
                    id,
                    m_layout->dimType(id)));
    }
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

} // namespace entwine


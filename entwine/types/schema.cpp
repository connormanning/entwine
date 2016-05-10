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

#include <numeric>

#include <entwine/types/simple-point-layout.hpp>

namespace entwine
{

namespace
{
    DimList makeDims(const Json::Value& json)
    {
        return std::accumulate(
                json.begin(),
                json.end(),
                DimList(),
                [](const DimList& in, const Json::Value& d)
                {
                    const auto sizeDim(d["size"]);

                    DimList out(in);
                    out.push_back(
                        DimInfo(
                            d["name"].asString(),
                            d["type"].asString(),
                            sizeDim.isIntegral() ?
                                sizeDim.asUInt64() :
                                std::stoul(sizeDim.asString())));

                    return out;
                });
    }

    std::unique_ptr<pdal::PointLayout> makePointLayout(DimList& dims)
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
    m_dims = makeDims(json);
    m_layout = makePointLayout(m_dims);
}

Schema::Schema(const std::string& s)
    : m_layout()
    , m_dims()
{
    Json::Value json;
    Json::Reader reader;

    if (reader.parse(s, json, false))
    {
        m_dims = makeDims(json);
        m_layout = makePointLayout(m_dims);
    }
    else
    {
        throw std::runtime_error(
                "Could not parse schema as JSON: " +
                reader.getFormattedErrorMessages());
    }
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
                DimInfo(m_layout->dimName(id), id, m_layout->dimType(id)));
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


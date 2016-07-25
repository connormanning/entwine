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

#include <cstddef>
#include <numeric>
#include <vector>

#include <pdal/PointLayout.hpp>

#include <entwine/third/json/json.hpp>
#include <entwine/types/dim-info.hpp>
#include <entwine/types/fixed-point-layout.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

class Schema
{
public:
    explicit Schema(DimList dims)
        : m_dims(dims)
        , m_layout(makePointLayout(m_dims))
    { }

    explicit Schema(const Json::Value& json)
        : Schema(
                std::accumulate(
                    json.begin(),
                    json.end(),
                    DimList(),
                    [](const DimList& in, const Json::Value& d)
                    {
                        DimList out(in);
                        out.emplace_back(d);
                        return out;
                    }))
    { }

    explicit Schema(const std::string& s) : Schema(parse(s)) { }

    template<typename T>
    Schema(std::initializer_list<T> il) : Schema(DimList(il)) { }

    Schema(const Schema& other) : Schema(other.m_dims) { }
    Schema& operator=(const Schema& other)
    {
        m_dims = other.m_dims;
        m_layout = makePointLayout(m_dims);
        return *this;
    }

    std::size_t pointSize() const
    {
        return m_layout->pointSize();
    }

    const DimList& dims() const
    {
        return m_dims;
    }

    bool contains(const std::string& name) const
    {
        const auto it(
                std::find_if(
                    m_dims.begin(),
                    m_dims.end(),
                    [&name](const DimInfo& d) { return d.name() == name; }));

        return it != m_dims.end();
    }

    pdal::Dimension::Id getId(const std::string& name) const
    {
        return pdalLayout().findDim(name);
    }

    pdal::PointLayout& pdalLayout() const
    {
        return *m_layout.get();
    }

    Json::Value toJson() const
    {
        return std::accumulate(
                m_dims.begin(),
                m_dims.end(),
                Json::Value(),
                [](const Json::Value& in, const DimInfo& d)
                {
                    Json::Value out(in);
                    out.append(d.toJson());
                    return out;
                });
    }

    std::string toString() const
    {
        return std::accumulate(
                m_dims.begin(),
                m_dims.end(),
                std::string(),
                [](const std::string& s, const DimInfo& d)
                {
                    return s + (s.size() ? ", " : "") + d.name();
                });
    }

private:
    std::unique_ptr<pdal::PointLayout> makePointLayout(DimList& dims)
    {
        std::unique_ptr<pdal::PointLayout> layout(new FixedPointLayout());

        for (auto& dim : dims)
        {
            dim.setId(layout->registerOrAssignDim(dim.name(), dim.type()));
        }

        layout->finalize();

        return layout;
    }

    DimList m_dims;
    std::unique_ptr<pdal::PointLayout> m_layout;
};

inline bool operator==(const Schema& lhs, const Schema& rhs)
{
    return lhs.dims() == rhs.dims();
}

inline bool operator!=(const Schema& lhs, const Schema& rhs)
{
    return !(lhs == rhs);
}

inline std::ostream& operator<<(std::ostream& os, const Schema& schema)
{
    os << schema.toJson();
    return os;
}

} // namespace entwine


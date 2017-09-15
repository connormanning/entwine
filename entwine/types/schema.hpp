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
#include <limits>
#include <memory>
#include <numeric>
#include <vector>

#include <pdal/PointLayout.hpp>

#include <json/json.h>

#include <entwine/types/delta.hpp>
#include <entwine/types/dim-info.hpp>
#include <entwine/types/fixed-point-layout.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

class Schema
{
public:
    Schema() : Schema(DimList()) { }

    explicit Schema(DimList dims)
    {
        auto push([this, &dims](std::string name)
        {
            auto comp([&name](const DimInfo& d) { return d.name() == name; });

            auto d(std::find_if(dims.begin(), dims.end(), comp));

            if (d != dims.end())
            {
                m_dims.push_back(*d);
                dims.erase(std::remove_if(dims.begin(), dims.end(), comp));
            }
        });

        push("X"); push("Y"); push("Z");
        for (const auto& dim : dims) m_dims.push_back(dim);
        m_layout = makePointLayout(m_dims);
    }

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

    bool empty() const { return pointSize() == 0; }
    std::size_t pointSize() const { return m_layout->pointSize(); }
    const DimList& dims() const { return m_dims; }

    bool contains(const std::string& name) const
    {
        const auto it(
                std::find_if(
                    m_dims.begin(),
                    m_dims.end(),
                    [&name](const DimInfo& d) { return d.name() == name; }));

        return it != m_dims.end();
    }

    bool contains(pdal::Dimension::Id id) const
    {
        const auto it(
                std::find_if(
                    m_dims.begin(),
                    m_dims.end(),
                    [&id](const DimInfo& d) { return d.id() == id; }));

        return it != m_dims.end();
    }

    bool hasColor() const
    {
        return contains("Red") || contains("Green") || contains("Blue");
    }

    bool hasTime() const
    {
        return contains("GpsTime");
    }

    const DimInfo& find(const std::string& name) const
    {
        const auto it(
                std::find_if(
                    m_dims.begin(),
                    m_dims.end(),
                    [&name](const DimInfo& d) { return d.name() == name; }));

        if (it != m_dims.end()) return *it;
        else throw std::runtime_error("Dimension not found: " + name);
    }

    const Schema filter(const std::string& name) const
    {
        DimList res;
        for (const auto& d : dims()) if (d.name() != name) res.push_back(d);
        return Schema(res);
    }

    const DimInfo& find(pdal::Dimension::Id id) const
    {
        const auto it(
                std::find_if(
                    m_dims.begin(),
                    m_dims.end(),
                    [&id](const DimInfo& d) { return d.id() == id; }));

        if (it != m_dims.end()) return *it;
        else throw std::runtime_error(
                "Dimension not found: " +
                std::to_string(pdal::Utils::toNative(id)));
    }

    pdal::Dimension::Id getId(const std::string& name) const
    {
        return pdalLayout().findDim(name);
    }

    pdal::PointLayout& pdalLayout() const
    {
        return *m_layout;
    }

    const FixedPointLayout& fixedLayout() const
    {
        if (const auto* f = static_cast<FixedPointLayout*>(m_layout.get()))
        {
            return *f;
        }
        else throw std::runtime_error("Layout is not a FixedPointLayout");
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

    bool normal() const
    {
        static const auto f(pdal::Dimension::BaseType::Floating);
        return
            pdal::Dimension::base(find("X").type()) == f &&
            pdal::Dimension::base(find("Y").type()) == f &&
            pdal::Dimension::base(find("Z").type()) == f;
    }

    static Schema normalize(const Schema& s)
    {
        DimList dims
        {
            pdal::Dimension::Id::X,
            pdal::Dimension::Id::Y,
            pdal::Dimension::Id::Z
        };

        for (const auto& dim : s.dims())
        {
            if (!DimInfo::isXyz(dim)) dims.emplace_back(dim);
        }

        return Schema(dims);
    };

    static Schema deltify(
            const Bounds& scaledCube,
            const Delta& delta,
            const Schema& inSchema)
    {
        pdal::Dimension::Type spatialType(pdal::Dimension::Type::Double);

        const Point ticks(
                scaledCube.width(),
                scaledCube.depth(),
                scaledCube.height());

        auto fitsWithin([&ticks](double max)
        {
            return ticks.x < max && ticks.y < max && ticks.z < max;
        });

        if (fitsWithin(std::numeric_limits<uint32_t>::max()))
        {
            spatialType = pdal::Dimension::Type::Signed32;
        }
        else if (fitsWithin(std::numeric_limits<uint64_t>::max()))
        {
            spatialType = pdal::Dimension::Type::Signed64;
        }
        else
        {
            std::cout << "Cannot use this scale for these bounds" << std::endl;
        }

        DimList dims
        {
            DimInfo(pdal::Dimension::Id::X, spatialType),
            DimInfo(pdal::Dimension::Id::Y, spatialType),
            DimInfo(pdal::Dimension::Id::Z, spatialType)
        };

        for (const auto& dim : inSchema.dims())
        {
            if (!DimInfo::isXyz(dim.id())) dims.emplace_back(dim);
        }

        return Schema(dims);
    }

    std::vector<pdal::Dimension::Id> ids() const
    {
        std::vector<pdal::Dimension::Id> v;
        for (const auto& d : m_dims) v.push_back(d.id());
        return v;
    }

    Schema append(const Schema& other) const
    {
        DimList d(dims());
        d.insert(d.end(), other.dims().begin(), other.dims().end());
        return Schema(d);
    }

private:
    std::unique_ptr<pdal::PointLayout> makePointLayout(DimList& dims)
    {
        std::unique_ptr<pdal::PointLayout> layout(new FixedPointLayout());

        for (auto& dim : dims)
        {
            dim.setId(layout->registerOrAssignDim(dim.name(), dim.type()));
            if (dim.id() == pdal::Dimension::Id::Unknown)
            {
                dim.setId(layout->findDim(dim.name()));
            }

            if (dim.id() == pdal::Dimension::Id::Unknown)
            {
                throw std::runtime_error(
                        "Could not register dimension " + dim.name());
            }
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
    os << "[";

    for (std::size_t i(0); i < schema.dims().size(); ++i)
    {
        const auto& d(schema.dims()[i]);
        os << "\n\t";
        os <<
            "{ \"name\": \"" << d.name() << "\"" <<
            ", \"type\": \"" << d.typeString() << "\"" <<
            ", \"size\": " << d.size() << " }";
        if (i != schema.dims().size() - 1) os << ",";
    }

    os << "\n]";
    return os;
}

} // namespace entwine


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

#include <entwine/types/dim-info.hpp>
#include <entwine/types/fixed-point-layout.hpp>
#include <entwine/types/scale-offset.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class Schema
{
public:
    Schema() = default;

    explicit Schema(DimList dims)
    {
        for (const auto& dim : dims) m_dims.push_back(dim);
        m_layout = makePointLayout(m_dims);
    }

    explicit Schema(const json& j)
        : Schema(j.get<DimList>())
    { }

    Schema(const Schema& other) : Schema(other.m_dims) { }
    Schema& operator=(const Schema& other)
    {
        m_dims = other.m_dims;
        m_layout = makePointLayout(m_dims);
        return *this;
    }

    bool empty() const { return pointSize() == 0; }
    bool exists() const { return !empty(); }
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

    bool contains(DimId id) const
    {
        const auto it(
                std::find_if(
                    m_dims.begin(),
                    m_dims.end(),
                    [&id](const DimInfo& d) { return d.id() == id; }));

        return it != m_dims.end();
    }

    bool isScaled() const
    {
        return
            find(DimId::X).isScaled() ||
            find(DimId::Y).isScaled() ||
            find(DimId::Z).isScaled();
    }

    void setScale(const Scale& s)
    {
        find(DimId::X).setScale(s.x);
        find(DimId::Y).setScale(s.y);
        find(DimId::Z).setScale(s.z);
    }

    void setOffset(const Offset& o)
    {
        find(DimId::X).setOffset(o.x);
        find(DimId::Y).setOffset(o.y);
        find(DimId::Z).setOffset(o.z);
    }

    void setScaleOffset(const Scale& s, const Offset& o)
    {
        setScale(s);
        setOffset(o);
    }

    Scale scale() const
    {
        return Scale(
                find(DimId::X).scale(),
                find(DimId::Y).scale(),
                find(DimId::Z).scale());
    }

    Offset offset() const
    {
        return Offset(
                find(DimId::X).offset(),
                find(DimId::Y).offset(),
                find(DimId::Z).offset());
    }

    std::unique_ptr<ScaleOffset> scaleOffset() const
    {
        if (isScaled()) return makeUnique<ScaleOffset>(scale(), offset());
        else return std::unique_ptr<ScaleOffset>();
    }

    std::unique_ptr<SingleScaleOffset> gpsScaleOffset() const
    {
        if (hasTime())
        {
            const DimInfo& g(find(DimId::GpsTime));
            if (g.scale() != 1.0 || g.offset() != 0.0)
            {
                return makeUnique<SingleScaleOffset>(g.scale(), g.offset());
            }
        }

        return std::unique_ptr<SingleScaleOffset>();
    }

    bool hasColor() const
    {
        return (
            contains(DimId::Red) ||
            contains(DimId::Green) ||
            contains(DimId::Blue));
    }

    bool hasTime() const
    {
        return contains(DimId::GpsTime);
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

    const Schema filter(DimId id) const
    {
        DimList res;
        for (const auto& d : dims()) if (d.id() != id) res.push_back(d);
        return Schema(res);
    }

    const Schema filter(const std::string& name) const
    {
        return filter(getId(name));
    }

    DimInfo& find(DimId id)
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

    const DimInfo& find(DimId id) const
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

    DimId getId(const std::string& name) const
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

    bool normal() const
    {
        static const auto f(pdal::Dimension::BaseType::Floating);
        return
            pdal::Dimension::base(find(DimId::X).type()) == f &&
            pdal::Dimension::base(find(DimId::Y).type()) == f &&
            pdal::Dimension::base(find(DimId::Z).type()) == f;
    }

    static Schema makeAbsolute(const Schema& s)
    {
        const Schema xyz(DimList {
            { DimId::X, DimType::Double },
            { DimId::Y, DimType::Double },
            { DimId::Z, DimType::Double }
        });

        Schema rest = s.filter(DimId::X).filter(DimId::Y).filter(DimId::Z);

        if (s.hasTime())
        {
            DimInfo& gps = rest.find(DimId::GpsTime);
            gps = DimInfo(DimId::GpsTime);
        }

        return xyz.merge(rest);
    };

    std::vector<DimId> ids() const
    {
        std::vector<DimId> v;
        for (const auto& d : m_dims) v.push_back(d.id());
        return v;
    }

    Schema append(const Schema& other) const
    {
        DimList d(dims());
        d.insert(d.end(), other.dims().begin(), other.dims().end());
        return Schema(d);
    }

    Schema append(const DimInfo& add) const
    {
        DimList d(dims());
        d.push_back(add);
        return Schema(d);
    }

    Schema merge(const Schema& other) const
    {
        Schema s(dims());
        for (const auto& d : other.dims())
        {
            if (!s.contains(d.name())) s = s.append(d);
        }
        return s;
    }

private:
    std::unique_ptr<pdal::PointLayout> makePointLayout(DimList& dims)
    {
        std::unique_ptr<pdal::PointLayout> layout(new FixedPointLayout());

        for (auto& dim : dims)
        {
            dim.setId(layout->registerOrAssignDim(dim.name(), dim.type()));
            if (dim.id() == DimId::Unknown)
            {
                dim.setId(layout->findDim(dim.name()));
            }

            if (dim.id() == DimId::Unknown)
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

inline void from_json(const json& j, Schema& s)
{
    s = Schema(j);
}

inline void to_json(json& j, const Schema& s)
{
    j = s.dims();
}

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
            ", \"size\": " << d.size();
        if (d.isScaled())
        {
            os <<
                ", \"scale\": " << d.scale() <<
                ", \"offset\": " << d.offset();
        }
        os << " }";
        if (i != schema.dims().size() - 1) os << ",";
    }

    os << "\n]";
    return os;
}

} // namespace entwine


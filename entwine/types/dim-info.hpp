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
#include <string>

#include <pdal/Dimension.hpp>

#include <entwine/types/defs.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

class Schema;

class DimInfo
{
    friend class Schema;

public:
    DimInfo() = default;

    DimInfo(DimId id)
        : DimInfo(id, defaultType(id))
    { }

    DimInfo(DimId id, DimType type, double scale = 1.0, double offset = 0.0)
        : DimInfo(pdal::Dimension::name(id), type, scale, offset)
    { }

    explicit DimInfo(std::string name)
        : DimInfo(name, defaultType(pdal::Dimension::id(name)))
    { }

    DimInfo(std::string name, std::string type, uint64_t size = 0)
        : DimInfo(name, getType(type, size))
    { }

    explicit DimInfo(const json& j)
        : DimInfo(j.value("name", ""), j.value("type", ""), j.value("size", 0))
    {
        if (j.count("scale"))  m_scale  = j.at("scale").get<double>();
        if (j.count("offset")) m_offset = j.at("offset").get<double>();
    }

    // All constructor overloads end up here.
    DimInfo(
            std::string name,
            DimType type,
            double scale = 1.0,
            double offset = 0.0)
        : m_name(name)
        , m_type(type)
        , m_id(pdal::Dimension::id(name))
        , m_scale(scale)
        , m_offset(offset)
    {
        if (m_name.empty())
        {
            throw std::runtime_error("Unnamed dimensions are not allowed");
        }

        if (m_type == DimType::None)
        {
            throw std::runtime_error("Typeless dimensions are not allowed");
        }
    }

    std::string name() const { return m_name; }
    std::string typeString() const
    {
        switch (base())
        {
            case pdal::Dimension::BaseType::Signed:   return "signed";
            case pdal::Dimension::BaseType::Unsigned: return "unsigned";
            case pdal::Dimension::BaseType::Floating: return "float";
            default: return "unknown";
        }
    }
    std::size_t size() const { return pdal::Dimension::size(type()); }

    DimId id() const { return m_id; }
    DimType type() const { return m_type; }
    pdal::Dimension::BaseType base() const
    {
        return pdal::Dimension::base(m_type);
    }

    double scale() const { return m_scale; }
    double offset() const { return m_offset; }
    bool isScaled() const { return m_scale != 1.0; }
    void setScale(double scale) { m_scale = scale; }
    void setOffset(double offset) { m_offset = offset; }
    void setScaleOffset(double scale, double offset)
    {
        m_scale = scale;
        m_offset = offset;
    }

    std::string typeName() const
    {
        switch (m_type)
        {
            case DimType::None:         return "unknown";
            case DimType::Signed8:      return "int8";
            case DimType::Signed16:     return "int16";
            case DimType::Signed32:     return "int32";
            case DimType::Signed64:     return "int64";
            case DimType::Unsigned8:    return "uint8";
            case DimType::Unsigned16:   return "uint16";
            case DimType::Unsigned32:   return "uint32";
            case DimType::Unsigned64:   return "uint64";
            case DimType::Float:        return "float";
            case DimType::Double:       return "double";
            default:                    return "unknown";
        }
    }

    static bool isXyz(const DimInfo& dim)
    {
        return isXyz(dim.id());
    }

    static bool isXyz(DimId id)
    {
        return id == DimId::X || id == DimId::Y || id == DimId::Z;
    }

private:
    // May be unknown until PDAL registration.
    void setId(DimId id) const { m_id = id; }

    DimType defaultType(DimId id) const
    {
        DimType t(DimType::Double);

        try { t = pdal::Dimension::defaultType(id); }
        catch (pdal::pdal_error&) { }

        return t;
    }

    DimType getType(const std::string type, uint64_t size) const
    {
        static const std::map<std::string, DimType> dimTypes {
            { "uint8",  DimType::Unsigned8 },
            { "uint16", DimType::Unsigned16 },
            { "uint32", DimType::Unsigned32 },
            { "uint64", DimType::Unsigned64 },
            { "int8",  DimType::Signed8 },
            { "int16", DimType::Signed16 },
            { "int32", DimType::Signed32 },
            { "int64", DimType::Signed64 },
            { "float", DimType::Float },
            { "double", DimType::Double }
        };

        if (!size && dimTypes.count(type))
        {
            return dimTypes.at(type);
        }
        if (type == "unsigned")
        {
            if (size == 1) return DimType::Unsigned8;
            if (size == 2) return DimType::Unsigned16;
            if (size == 4) return DimType::Unsigned32;
            if (size == 8) return DimType::Unsigned64;
        }
        if (type == "signed")
        {
            if (size == 1) return DimType::Signed8;
            if (size == 2) return DimType::Signed16;
            if (size == 4) return DimType::Signed32;
            if (size == 8) return DimType::Signed64;
        }
        if (type == "float" || type == "floating")
        {
            if (size == 4) return DimType::Float;
            if (size == 8) return DimType::Double;
        }

        throw std::runtime_error("Invalid dimension specification");
    }

    std::string m_name;
    DimType m_type;
    mutable DimId m_id = DimId::Unknown;

    double m_scale = 1.0;
    double m_offset = 0.0;
};

inline void from_json(const json& j, DimInfo& d)
{
    d = DimInfo(j);
}

inline void to_json(json& j, const DimInfo& d)
{
    j = json {
        { "name", d.name() },
        { "type", d.typeString() },
        { "size", d.size() }
    };

    if (d.scale() != 1.0) j["scale"] = d.scale();
    if (d.offset() != 0.0) j["offset"] = d.offset();
}

using DimList = std::vector<DimInfo>;

inline bool operator==(const DimInfo& a, const DimInfo& b)
{
    return a.name() == b.name() &&
        a.type() == b.type() &&
        a.scale() == b.scale() &&
        a.offset() == b.offset();
}

inline bool operator!=(const DimInfo& lhs, const DimInfo& rhs)
{
    return !(lhs == rhs);
}

} // namespace entwine


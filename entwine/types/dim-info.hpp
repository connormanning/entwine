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

#include <json/json.h>

namespace entwine
{

class Schema;

class DimInfo
{
    friend class Schema;

public:
    DimInfo(pdal::Dimension::Id id)
        : DimInfo(id, pdal::Dimension::defaultType(id))
    { }

    DimInfo(pdal::Dimension::Id id, pdal::Dimension::Type type)
        : DimInfo(pdal::Dimension::name(id), id, type)
    { }

    DimInfo(
            const std::string& name,
            const std::string& baseTypeName,
            std::size_t size = 0)
        : DimInfo(
                name,
                pdal::Dimension::Id::Unknown,
                getType(baseTypeName, size))
    { }

    DimInfo(const std::string& name)
        : DimInfo(pdal::Dimension::id(name))
    { }

    DimInfo(const Json::Value& json)
        : DimInfo(
                json["name"].asString(),
                json["type"].asString(),
                json["size"].isIntegral() ?
                    json["size"].asUInt64() :
                    std::stoul(json["size"].asString()))
    { }

    DimInfo(
            const std::string& name,
            pdal::Dimension::Id id,
            pdal::Dimension::Type type)
        : m_name(name)
        , m_id(id)
        , m_type(type)
    { }

    std::string name() const { return m_name; }
    std::size_t size() const { return pdal::Dimension::size(type()); }
    std::string typeString() const
    {
        return pdal::Dimension::toName(pdal::Dimension::base(m_type));
    }

    pdal::Dimension::Id id() const { return m_id; }
    pdal::Dimension::Type type() const { return m_type; }

    Json::Value toJson() const
    {
        Json::Value json;
        json["name"] = name();
        json["type"] = typeString();
        json["size"] = static_cast<Json::UInt64>(size());
        return json;
    }

    static bool isXyz(const DimInfo& dim)
    {
        return isXyz(dim.id());
    }

    static bool isXyz(pdal::Dimension::Id id)
    {
        return
            id == pdal::Dimension::Id::X ||
            id == pdal::Dimension::Id::Y ||
            id == pdal::Dimension::Id::Z;
    }

    // May be unknown until PDAL registration.
    void setId(pdal::Dimension::Id id) const { m_id = id; }

private:
    std::string m_name;
    mutable pdal::Dimension::Id m_id;
    pdal::Dimension::Type m_type;
    std::string m_typeString;

    pdal::Dimension::Type getType(
            const std::string& type,
            const std::size_t size)
    {
        using D = pdal::Dimension::Type;

        if (type == "floating")
        {
            if      (size == 4) return D::Float;
            else if (size == 8) return D::Double;
        }
        if (type == "unsigned")
        {
            if      (size == 1) return D::Unsigned8;
            else if (size == 2) return D::Unsigned16;
            else if (size == 4) return D::Unsigned32;
            else if (size == 8) return D::Unsigned64;
        }
        if (type == "signed")
        {
            if      (size == 1) return D::Signed8;
            else if (size == 2) return D::Signed16;
            else if (size == 4) return D::Signed32;
            else if (size == 8) return D::Signed64;
        }

        if (!size)
        {
            if (type == "float32") return D::Float;
            if (type == "float64") return D::Double;
            if (type == "uint8") return D::Unsigned8;
            if (type == "uint16") return D::Unsigned16;
            if (type == "uint32") return D::Unsigned32;
            if (type == "uint64") return D::Unsigned64;
            if (type == "int8") return D::Signed8;
            if (type == "int16") return D::Signed16;
            if (type == "int32") return D::Signed32;
            if (type == "int64") return D::Signed64;
        }

        throw std::runtime_error("Invalid type specification");
    }
};

using DimList = std::vector<DimInfo>;

inline bool operator==(const DimInfo& lhs, const DimInfo& rhs)
{
    return lhs.name() == rhs.name() && lhs.type() == rhs.type();
}

inline bool operator!=(const DimInfo& lhs, const DimInfo& rhs)
{
    return !(lhs == rhs);
}

} // namespace entwine


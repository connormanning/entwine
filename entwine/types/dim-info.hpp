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

    DimInfo(const std::string& name, const std::string& type)
        : DimInfo(
                name,
                pdal::Dimension::Id::Unknown,
                pdal::Dimension::type(type))
    { }

    DimInfo(const std::string& name)
        : DimInfo(pdal::Dimension::id(name))
    { }

    DimInfo(const Json::Value& json)
        : DimInfo(json["name"].asString(), json["type"].asString())
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
        using Type = pdal::Dimension::Type;
        switch (m_type)
        {
            case Type::None:        return "unknown";
            case Type::Signed8:     return "int8";
            case Type::Signed16:    return "int16";
            case Type::Signed32:    return "int32";
            case Type::Signed64:    return "int64";
            case Type::Unsigned8:   return "uint8";
            case Type::Unsigned16:  return "uint16";
            case Type::Unsigned32:  return "uint32";
            case Type::Unsigned64:  return "uint64";
            case Type::Float:       return "float";
            case Type::Double:      return "double";
            default:                return "unknown";
        }
    }

    pdal::Dimension::Id id() const { return m_id; }
    pdal::Dimension::Type type() const { return m_type; }

    Json::Value toJson() const
    {
        Json::Value json;
        json["name"] = name();
        json["type"] = typeString();
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


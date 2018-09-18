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

#include <entwine/types/defs.hpp>

namespace entwine
{

class Schema;

class DimInfo
{
    friend class Schema;

public:
    DimInfo(std::string name)
        : DimInfo(pdal::Dimension::id(name))
    { }

    DimInfo(DimId id)
        : DimInfo(id, pdal::Dimension::defaultType(id))
    { }

    DimInfo(DimId id, DimType type)
        : DimInfo(pdal::Dimension::name(id), type, id)
    { }

    DimInfo(std::string name, DimType type, DimId id = DimId::Unknown)
        : m_name(name)
        , m_type(type)
        , m_id(id)
    { }

    DimInfo(std::string name, std::string type, uint64_t size = 0)
        : DimInfo(name, getType(type, size))
    { }

    DimInfo(const Json::Value& json)
        : DimInfo(
                json["name"].asString(),
                json["type"].asString(),
                json["size"].asUInt64())
    { }

    std::string name() const { return m_name; }
    std::string typeString() const
    {
        switch (base())
        {
            case pdal::Dimension::BaseType::Signed: return "signed";
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

    Json::Value toJson() const
    {
        Json::Value json;
        json["name"] = name();
        json["type"] = typeString();
        json["size"] = static_cast<Json::UInt64>(size());
        return json;
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
        return
            id == DimId::X ||
            id == DimId::Y ||
            id == DimId::Z;
    }

private:
    // May be unknown until PDAL registration.
    void setId(DimId id) const { m_id = id; }

    DimType getType(const std::string type, uint64_t size)
        const
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
    mutable DimId m_id;
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


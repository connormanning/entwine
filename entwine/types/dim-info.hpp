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

#include <entwine/third/json/json.hpp>

namespace entwine
{

class Schema;

class DimInfo
{
    friend class Schema;

public:
    DimInfo(pdal::Dimension::Id::Enum id)
        : DimInfo(id, pdal::Dimension::defaultType(id))
    { }

    DimInfo(pdal::Dimension::Id::Enum id, pdal::Dimension::Type::Enum type)
        : DimInfo(pdal::Dimension::name(id), id, type)
    { }

    DimInfo(
            const std::string& name,
            const std::string& baseTypeName,
            std::size_t size)
        : DimInfo(
                name,
                pdal::Dimension::Id::Unknown,
                getType(baseTypeName, size))
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
            pdal::Dimension::Id::Enum id,
            pdal::Dimension::Type::Enum type)
        : m_name(name)
        , m_id(id)
        , m_type(type)
        , m_typeString(pdal::Dimension::toName(pdal::Dimension::base(m_type)))
    { }

    std::string name() const { return m_name; }
    std::size_t size() const { return pdal::Dimension::size(type()); }
    std::string typeString() const { return m_typeString; }

    pdal::Dimension::Id::Enum id() const { return m_id; }
    pdal::Dimension::Type::Enum type() const { return m_type; }

    Json::Value toJson() const
    {
        Json::Value json;
        json["name"] = name();
        json["type"] = typeString();
        json["size"] = static_cast<Json::UInt64>(size());
        return json;
    }

private:
    std::string m_name;
    pdal::Dimension::Id::Enum m_id;
    pdal::Dimension::Type::Enum m_type;
    std::string m_typeString;

    // May be unknown until PDAL registration.
    void setId(pdal::Dimension::Id::Enum id) { m_id = id; }

    pdal::Dimension::Type::Enum getType(
            const std::string& baseTypeName,
            std::size_t size)
    {
        if (baseTypeName == "floating")
        {
            if      (size == 4) return pdal::Dimension::Type::Float;
            else if (size == 8) return pdal::Dimension::Type::Double;
        }
        if (baseTypeName == "unsigned")
        {
            if      (size == 1) return pdal::Dimension::Type::Unsigned8;
            else if (size == 2) return pdal::Dimension::Type::Unsigned16;
            else if (size == 4) return pdal::Dimension::Type::Unsigned32;
            else if (size == 8) return pdal::Dimension::Type::Unsigned64;
        }
        else if (baseTypeName == "signed")
        {
            if      (size == 1) return pdal::Dimension::Type::Signed8;
            else if (size == 2) return pdal::Dimension::Type::Signed16;
            else if (size == 4) return pdal::Dimension::Type::Signed32;
            else if (size == 8) return pdal::Dimension::Type::Signed64;
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


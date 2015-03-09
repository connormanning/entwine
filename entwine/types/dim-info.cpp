/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/dim-info.hpp>

namespace
{
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
}

namespace entwine
{

DimInfo::DimInfo(
        const std::string& name,
        pdal::Dimension::Id::Enum id,
        pdal::Dimension::Type::Enum type)
    : m_name(name)
    , m_id(id)
    , m_type(type)
    , m_typeString(pdal::Dimension::toName(pdal::Dimension::base(type)))
{ }

DimInfo::DimInfo(
        const std::string& name,
        const std::string& baseTypeName,
        const std::size_t size)
    : m_name(name)
    , m_id(pdal::Dimension::Id::Unknown)
    , m_type(getType(baseTypeName, size))
    , m_typeString(baseTypeName)
{ }

std::string DimInfo::name() const
{
    return m_name;
}

pdal::Dimension::Id::Enum DimInfo::id() const
{
    return m_id;
}

pdal::Dimension::Type::Enum DimInfo::type() const
{
    return m_type;
}

std::size_t DimInfo::size() const
{
    return pdal::Dimension::size(type());
}

std::string DimInfo::typeString() const
{
    return m_typeString;
}

void DimInfo::setId(pdal::Dimension::Id::Enum id)
{
    m_id = id;
}

} // namespace entwine


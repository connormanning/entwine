/******************************************************************************
* Copyright (c) 2018, Andrew Polden
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <string>
#include <vector>
#include <unordered_map>

#include <pdal/DimUtil.hpp>

#include <json/json.h>

namespace entwine
{
namespace cesium
{

class BatchReference
{
public:
    enum class ComponentType
    {
        Byte,
        UnsignedByte,
        Short,
        UnsignedShort,
        Int,
        UnsignedInt,
        Float,
        Double
    };

    enum class Type
    {
        Scalar,
        Vec2,
        Vec3,
        Vec4,
    };

    BatchReference(
            std::string propertyName,
            std::size_t byteOffset,
            const ComponentType componentType,
            const Type type = Type::Scalar)
        : m_propertyName(propertyName)
        , m_byteOffset(byteOffset)
        , m_componentType(componentType)
        , m_type(type)
    { }

    std::string name() const { return m_propertyName; };
    std::size_t byteOffset() const { return m_byteOffset; };
    Json::Value getJson() const;
    std::size_t bytes() const;

    static const ComponentType findComponentType(pdal::Dimension::Type);

private:
    const std::string m_propertyName;
    const std::size_t m_byteOffset;
    const ComponentType m_componentType;
    const Type m_type;
};

} // namespace cesium
} // namespace entwine


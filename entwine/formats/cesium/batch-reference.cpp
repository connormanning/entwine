/******************************************************************************
* Copyright (c) 2018, Andrew Polden
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/formats/cesium/batch-reference.hpp>

#include <json/json.h>

namespace entwine
{
namespace cesium
{

namespace
{

using DT = pdal::Dimension::Type;
using CT = BatchReference::ComponentType;
using T = BatchReference::Type;

struct EnumHash
{
    template<typename T> std::size_t operator()(T t) const
    {
        return pdal::Utils::toNative(t);
    }
};

// Mappings used for converting between the PDAL dimension type, the batch
// reference types, and their respective string representation and size.
const std::unordered_map<const DT, const CT, EnumHash> componentTypes = {
    { DT::Signed8, CT::Byte },
    { DT::Unsigned8, CT::UnsignedByte },
    { DT::Signed16, CT::Short },
    { DT::Unsigned16, CT::UnsignedShort },
    { DT::Signed32, CT::Int },
    { DT::Unsigned32, CT::UnsignedInt },
    { DT::Float, CT::Float },
    { DT::Double, CT::Double }
};

const std::unordered_map<const CT, const char*, EnumHash> componentTypeNames = {
    { CT::Byte, "BYTE" },
    { CT::UnsignedByte, "UNSIGNED_BYTE" },
    { CT::Short, "SHORT" },
    { CT::UnsignedShort, "UNSIGNED_SHORT" },
    { CT::Int, "INT" },
    { CT::UnsignedInt, "UNSIGNED_INT" },
    { CT::Float, "FLOAT" },
    { CT::Double, "DOUBLE" }
};

const std::unordered_map<const CT, std::uint8_t, EnumHash> componentTypeSizes = {
    { CT::Byte, 1 },
    { CT::UnsignedByte, 1 },
    { CT::Short, 2 },
    { CT::UnsignedShort, 2 },
    { CT::Int, 4 },
    { CT::UnsignedInt, 4 },
    { CT::Float, 4 },
    { CT::Double, 8 }
};

const std::unordered_map<const T, const char*, EnumHash> typeNames = {
    { T::Scalar, "SCALAR" },
    { T::Vec2, "VEC2" },
    { T::Vec3, "VEC3" },
    { T::Vec4, "VEC4" }
};

const std::unordered_map<const T, std::uint8_t, EnumHash> typeSizes = {
    { T::Scalar, 1 },
    { T::Vec2, 2 },
    { T::Vec3, 3 },
    { T::Vec4, 4 }
};

}

Json::Value BatchReference::getJson() const
{
    Json::Value json;
    json["byteOffset"] = Json::UInt64(m_byteOffset);
    json["componentType"] = componentTypeNames.at(m_componentType);
    json["type"] = typeNames.at(m_type);

    return json;
};

std::size_t BatchReference::bytes() const
{
    return componentTypeSizes.at(m_componentType) * typeSizes.at(m_type);
};

const CT BatchReference::findComponentType(DT type)
{
    const auto it(componentTypes.find(type));

    if (it == componentTypes.end())
    {
        throw std::runtime_error(
                "Dimension type not supported by batch table.");
    }

    return it->second;
};


} // namespace cesium
} // namespace entwine


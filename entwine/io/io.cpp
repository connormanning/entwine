/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/io/io.hpp>

#include <stdexcept>

namespace entwine
{
namespace io
{

Type toType(const std::string s)
{
    if (s == "binary") return Type::Binary;
    if (s == "laszip") return Type::Laszip;
    if (s == "zstandard") return Type::Zstandard;
    throw std::runtime_error("Invalid data IO type: " + s);
}

std::string toString(const Type t)
{
    if (t == Type::Binary) return "binary";
    if (t == Type::Laszip) return "laszip";
    if (t == Type::Zstandard) return "zstandard";
    throw std::runtime_error("Invalid data IO enumeration");
}

} // namespace io
} // namespace entwine

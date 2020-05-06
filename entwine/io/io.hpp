/******************************************************************************
* Copyright (c) 2020, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <entwine/types/endpoints.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/json.hpp>

#include <entwine/io/binary.hpp>
#include <entwine/io/laszip.hpp>
#include <entwine/io/zstandard.hpp>

namespace entwine
{

struct Metadata;

namespace io
{

enum class Type { Binary, Laszip, Zstandard };

Type toType(std::string s);
std::string toString(Type t);
inline void to_json(json& j, Type t) { j = toString(t); }
inline void from_json(const json& j, Type& t)
{
    t = toType(j.get<std::string>());
}

template <typename... Args>
void write(Type type, Args&&... args)
{
    auto f = ([type]()
    {
        if (type == Type::Binary) return binary::write;
        if (type == Type::Laszip) return laszip::write;
        if (type == Type::Zstandard) return zstandard::write;
        throw std::runtime_error("Invalid data type");
    })();

    f(std::forward<Args>(args)...);
}

template <typename... Args>
void read(Type type, Args&&... args)
{
    auto f = ([type]()
    {
        if (type == Type::Binary) return binary::read;
        if (type == Type::Laszip) return laszip::read;
        if (type == Type::Zstandard) return zstandard::read;
        throw std::runtime_error("Invalid data type");
    })();

    f(std::forward<Args>(args)...);
}

} // namespace io
} // namespace entwine

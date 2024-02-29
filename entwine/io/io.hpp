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

#include <entwine/types/bounds.hpp>
#include <entwine/types/endpoints.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

struct Metadata;

struct Io
{
    Io(
        const Metadata& metadata,
        const Endpoints& endpoints)
        : metadata(metadata)
        , endpoints(endpoints)
    { }
    virtual ~Io() { }

    static std::unique_ptr<Io> create(
        const Metadata& metadata,
        const Endpoints& endpoints);

    virtual void write(
        std::string filename,
        BlockPointTable& table,
        const Bounds bounds) const = 0;

    virtual void read(std::string filename, VectorPointTable& table) const = 0;

    const Metadata& metadata;
    const Endpoints& endpoints;
};

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

} // namespace io
} // namespace entwine

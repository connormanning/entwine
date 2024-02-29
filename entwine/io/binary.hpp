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

#include <entwine/io/io.hpp>

namespace entwine
{
namespace io
{

struct Binary : public Io
{
    Binary(const Metadata& metadata, const Endpoints& endpoints)
        : Io(metadata, endpoints)
    { }

    virtual void write(
        std::string filename,
        BlockPointTable& table,
        const Bounds bounds) const override;

    void read(std::string filename, VectorPointTable& table) const override;
};

namespace binary
{

std::vector<char> pack(const Metadata& metadata, BlockPointTable& src);
void unpack(
    const Metadata& m,
    VectorPointTable& dst,
    std::vector<char>&& buffer);

} // namespace binary
} // namespace io
} // namespace entwine

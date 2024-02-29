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

struct Laszip : public Io
{
    Laszip(const Metadata& metadata, const Endpoints& endpoints)
        : Io(metadata, endpoints)
    { }

    virtual void write(
        std::string filename,
        BlockPointTable& table,
        const Bounds bounds) const override;

    void read(std::string filename, VectorPointTable& table) const override;
};

} // namespace io
} // namespace entwine

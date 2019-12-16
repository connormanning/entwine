/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
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

#include <entwine/types/metadata.hpp>
#include <entwine/types/vector-point-table.hpp>

namespace entwine
{

class DataIo
{
public:
    DataIo(const Metadata& metadata) : m_metadata(metadata) { }
    virtual ~DataIo() { }

    static std::unique_ptr<DataIo> create(const Metadata& m, std::string type);

    virtual std::string type() const = 0;

    virtual void write(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            const std::string& filename,
            const Bounds& bounds,
            BlockPointTable& table) const
    { }

    virtual void read(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            const std::string& filename,
            VectorPointTable& table) const
    { }

protected:
    const Metadata& m_metadata;
};

} // namespace entwine


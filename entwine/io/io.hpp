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

#include <json/json.h>

#include <entwine/io/ensure.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point-pool.hpp>

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
            PointPool& pointPool,
            const std::string& filename,
            Cell::PooledStack&& cells,
            uint64_t np) const = 0;

    virtual Cell::PooledStack read(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            PointPool& pointPool,
            const std::string& filename) const = 0;

protected:
    const Metadata& m_metadata;
};

} // namespace entwine


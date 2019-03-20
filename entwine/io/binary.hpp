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

#include <entwine/io/io.hpp>

#include <entwine/types/binary-point-table.hpp>

namespace entwine
{

class Binary : public DataIo
{
public:
    Binary(const Metadata& m) : DataIo(m) { }

    virtual std::string type() const override { return "binary"; }

    virtual void write(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            const std::string& filename,
            const Bounds& bounds,
            BlockPointTable& table) const override;

    virtual void read(
            const arbiter::Endpoint& out,
            const arbiter::Endpoint& tmp,
            const std::string& filename,
            VectorPointTable& table) const override;

protected:
    std::vector<char> pack(BlockPointTable& src) const;
    void unpack(VectorPointTable& dst, std::vector<char>&& buffer) const;
};

} // namespace entwine


/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/subset.hpp>

#include <cassert>

namespace entwine
{

Subset::Subset(const uint64_t id, const uint64_t of)
    : id(id)
    , of(of)
{
    if (!id) throw std::runtime_error("Subset IDs should be 1-based.");
    if (of <= 1) throw std::runtime_error("Invalid subset range");
    if (id > of) throw std::runtime_error("Invalid subset ID - too large.");

    if (std::pow(2, static_cast<uint64_t>(std::log2(of))) != of)
    {
        throw std::runtime_error("Subset range must be a power of 2");
    }

    if (std::pow(static_cast<uint64_t>(std::sqrt(of)), 2) != of)
    {
        throw std::runtime_error("Subset range must be a perfect square");
    }
}

Bounds getBounds(Bounds cube, const Subset& s)
{
    assert(s.id);

    const uint64_t mask(0x3);
    for (std::size_t i(0); i < getSplits(s); ++i)
    {
        const Dir dir(toDir(((s.id - 1) >> (i * 2)) & mask));
        cube.go(dir, true);
    }
    return cube;
}


} // namespace entwine


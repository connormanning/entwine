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

#include <entwine/types/metadata.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Subset::Subset(const Bounds cube, const json& j)
    : m_id(j.at("id").get<uint64_t>())
    , m_of(j.at("of").get<uint64_t>())
    , m_splits(std::log2(m_of) / std::log2(4))
    , m_bounds(cube)
{
    if (!m_id) throw std::runtime_error("Subset IDs should be 1-based.");
    if (m_of <= 1) throw std::runtime_error("Invalid subset range");
    if (m_id > m_of) throw std::runtime_error("Invalid subset ID - too large.");

    if (std::pow(2, static_cast<uint64_t>(std::log2(m_of))) != m_of)
    {
        throw std::runtime_error("Subset range must be a power of 2");
    }

    if (std::pow(static_cast<uint64_t>(std::sqrt(m_of)), 2) != m_of)
    {
        throw std::runtime_error("Subset range must be a perfect square");
    }

    // Always split only X-Y range, leaving Z at its full extents.
    const uint64_t mask(0x3);
    for (std::size_t i(0); i < m_splits; ++i)
    {
        const Dir dir(toDir(((m_id - 1) >> (i * 2)) & mask));
        m_bounds.go(dir, true);
    }
}

std::unique_ptr<Subset> Subset::create(const Bounds cube, const json& j)
{
    if (j.is_null()) return std::unique_ptr<Subset>();
    else return makeUnique<Subset>(cube, j);
}

} // namespace entwine


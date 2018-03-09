/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/new-clipper.hpp>

#include <entwine/tree/registry.hpp>

namespace entwine
{

void NewClipper::clip(const bool force)
{
    const std::size_t start(m_registry.metadata().structure().head());
    for (std::size_t d(start); d < m_clips.size(); ++d)
    {
        auto& c(m_clips[d]);
        if (c.empty()) return;
        else c.clip(d, force);
    }
}

void NewClipper::clip(
        const uint64_t d,
        const uint64_t x,
        const uint64_t y,
        const uint64_t z)
{
    m_registry.clip(d, x, y, z, m_origin);
}

} // namespace entwine


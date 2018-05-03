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
#include <entwine/util/time.hpp>

namespace entwine
{

void NewClipper::clip()
{
    if (m_count <= heuristics::clipCacheSize) return;
    const auto startTime(now());
    const auto n(m_count);

    const std::size_t tail(m_registry.metadata().structure().tail());

    std::size_t cur(tail);
    while (cur < m_clips.size() && !m_clips[cur].empty()) ++cur;
    --cur; // We've gone one past the last - back it up by one.

    while (cur >= tail && m_count > heuristics::clipCacheSize)
    {
        auto& c(m_clips[cur]);
        if (c.empty()) return;
        else m_count -= c.clip(cur);

        --cur;
    }

    if (n - m_count)
    {
        std::cout << "  C " << n - m_count << "/" << m_count << " in " <<
            since<std::chrono::milliseconds>(startTime) << "ms" << std::endl;
    }
}

void NewClipper::clipAll()
{
    const std::size_t start(m_registry.metadata().structure().head());
    for (std::size_t d(start); d < m_clips.size(); ++d)
    {
        auto& c(m_clips[d]);
        if (c.empty()) return;
        else m_count -= c.clip(d, true);
    }

    assert(m_count == 0);
}

void NewClipper::clip(const uint64_t d, const Xyz& p)
{
    m_registry.clip(d, p, m_origin);
}

} // namespace entwine


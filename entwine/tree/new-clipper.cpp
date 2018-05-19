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
#include <entwine/tree/self-chunk.hpp>
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
        else m_count -= c.newClip(cur);

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
    std::cout << "Clipall" << std::endl;
    const std::size_t start(m_registry.metadata().structure().head());
    for (std::size_t d(m_clips.size() - 1); d >= start; --d)
    {
        auto& c(m_clips[d]);
        m_count -= c.newClip(true);
    }
    assert(!m_count);
}

bool NewClipper::insert(ReffedSelfChunk& c)
{
    const bool added(m_clips.at(c.key().depth()).insert(c));
    if (added) ++m_count;
    return added;
}

bool NewClipper::Clip::insert(ReffedSelfChunk& c)
{
    const auto it(m_chunks.find(&c));
    if (it == m_chunks.end())
    {
        m_chunks[&c] = true;
        return true;
    }
    else
    {
        it->second = true;
        return false;
    }
}

std::size_t NewClipper::Clip::newClip(const bool force)
{
    std::size_t n(0);
    for (auto it(m_chunks.begin()); it != m_chunks.end(); )
    {
        if (force || !it->second)
        {
            ReffedSelfChunk& c(*it->first);
            c.unref(m_clipper.origin());
            ++n;
            it = m_chunks.erase(it);
        }
        else
        {
            it->second = false;
            ++it;
        }
    }
    return n;
}

void NewClipper::clip(const uint64_t d, const Xyz& p)
{
    m_registry.clip(d, p, m_origin);
}

} // namespace entwine


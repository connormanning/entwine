/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/builder/clipper.hpp>

#include <entwine/builder/chunk.hpp>
#include <entwine/builder/registry.hpp>
#include <entwine/util/time.hpp>

namespace entwine
{

bool Clipper::insert(ReffedChunk& c)
{
    const bool added(m_clips.at(c.key().depth()).insert(c));
    if (added) ++m_count;
    return added;
}

void Clipper::clip()
{
    if (m_count <= heuristics::clipCacheSize) return;
    const auto startTime(now());
    const auto n(m_count);

    const std::size_t body(m_registry.metadata().structure().body());

    std::size_t cur(body);
    while (cur < m_clips.size() && !m_clips[cur].empty()) ++cur;
    --cur; // We've gone one past the last - back it up by one.

    while (cur >= body && m_count > heuristics::clipCacheSize)
    {
        auto& c(m_clips[cur]);
        if (c.empty()) return;
        else m_count -= c.clip();

        --cur;
    }

    if (n - m_count)
    {
        std::cout << "  C " << n - m_count << "/" << m_count << " in " <<
            since<std::chrono::milliseconds>(startTime) << "ms" << std::endl;
    }
}

void Clipper::clipAll()
{
    const std::size_t start(m_registry.metadata().structure().head());
    for (std::size_t d(m_clips.size() - 1); d >= start; --d)
    {
        auto& c(m_clips[d]);
        m_count -= c.clip(true);
    }
    assert(!m_count);
}

bool Clipper::Clip::insert(ReffedChunk& c)
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

bool Clipper::Clip::Cmp::operator()(
        const ReffedChunk* a,
        const ReffedChunk* b) const
{
    return a->key().position() < b->key().position();
}

std::size_t Clipper::Clip::clip(const bool force)
{
    std::size_t n(0);
    for (auto it(m_chunks.begin()); it != m_chunks.end(); )
    {
        if (force || !it->second)
        {
            ReffedChunk& c(*it->first);
            const Origin o(m_clipper.origin());
            m_clipper.registry().clipPool().add([&c, o] { c.unref(o); });
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

} // namespace entwine


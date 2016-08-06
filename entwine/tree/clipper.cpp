/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/clipper.hpp>
#include <entwine/tree/heuristics.hpp>

namespace entwine
{

void Clipper::clip()
{
    if (m_clips.size() < heuristics::clipCacheSize) return;

    std::cout << "Clipping" << std::endl;
    const std::size_t start(m_clips.size());

    m_fastCache.assign(32, m_clips.end());
    bool done(false);

    while (m_clips.size() > heuristics::clipCacheSize && !done)
    {
        ClipInfo::Map::iterator& it(*m_order.rbegin());

        if (!it->second.fresh)
        {
            m_builder.clip(it->first, it->second.chunkNum, m_id);
            m_clips.erase(it);
            m_order.pop_back();
        }
        else
        {
            done = true;
        }
    }

    std::cout << "Clipped: " << (start - m_clips.size()) << std::endl;

    for (auto& p : m_clips) p.second.fresh = false;
}

void Clipper::clip(const Id& chunkId)
{
    m_builder.clip(chunkId, m_clips.at(chunkId).chunkNum, m_id, true);
    m_clips.erase(chunkId);
}

} // namespace entwine


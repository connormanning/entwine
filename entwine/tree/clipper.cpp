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

namespace entwine
{

bool Clipper::insert(const Id& chunkId, std::size_t chunkNum)
{
    const auto find(m_clips.find(chunkId));

    if (find != m_clips.end())
    {
        find->second.fresh = true;
        return false;
    }
    else
    {
        m_clips.insert(std::make_pair(chunkId, ClipInfo(chunkNum)));
        return true;
    }
}

void Clipper::clip()
{
    if (m_clips.size() < 10) return;

    auto it(m_clips.begin());

    while (it != m_clips.end())
    {
        if (it->second.fresh)
        {
            it->second.fresh = false;
            ++it;
        }
        else
        {
            m_builder.clip(it->first, it->second.chunkNum, m_id);
            it = m_clips.erase(it);
        }
    }
}

} // namespace entwine


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

void NewClipper::clip()
{
    for (const auto& clip : m_clips)
    {
        m_registry.clip(clip.d, clip.x, clip.y, m_origin);
    }
}

} // namespace entwine


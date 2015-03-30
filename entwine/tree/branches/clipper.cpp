/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/branches/clipper.hpp>

#include <entwine/tree/branch.hpp>
#include <entwine/tree/sleepy-tree.hpp>

namespace entwine
{

Clipper::Clipper(SleepyTree& tree)
    : m_tree(tree)
    , m_clips()
{ }

Clipper::~Clipper()
{
    clip();
}

bool Clipper::insert(const std::size_t index)
{
    return m_clips.insert(index).second;
}

void Clipper::clip()
{
    for (auto index : m_clips)
    {
        m_tree.clip(this, index);
    }

    m_clips.clear();
}

} // anonymous namespace


/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/branches/trimmer.hpp>

#include <entwine/tree/branch.hpp>

namespace entwine
{

Trimmer::Trimmer()
    : m_clips()
{ }

Trimmer::~Trimmer()
{
    trim();
}

void Trimmer::trim()
{
    for (auto& clip : m_clips)
    {
        Branch* branch(clip.first);
        branch->purge(clip.second);
    }
}

} // anonymous namespace


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

#include <entwine/tree/builder.hpp>

namespace entwine
{

Clipper::Clipper(Builder& builder)
    : m_builder(builder)
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
        m_builder.clip(index, this);
    }

    m_clips.clear();
}

} // anonymous namespace


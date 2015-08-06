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

bool Clipper::insert(const Id& chunkId, const std::size_t chunkNum)
{
    return m_clips.insert(IdInfo(chunkId, chunkNum)).second;
}

void Clipper::clip()
{
    for (const auto& info : m_clips)
    {
        m_builder.clip(info.chunkId, info.chunkNum, this);
    }

    m_clips.clear();
}

} // anonymous namespace


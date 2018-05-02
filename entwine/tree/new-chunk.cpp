/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/new-chunk.hpp>

#include <cstddef>
#include <mutex>

namespace entwine
{

namespace
{
    std::mutex m;
    std::size_t n(0);
}

NewChunk::NewChunk()
{
    std::lock_guard<std::mutex> lock(m);
    ++n;
}

NewChunk::~NewChunk()
{
    std::lock_guard<std::mutex> lock(m);
    --n;
}

std::size_t NewChunk::count()
{
    std::lock_guard<std::mutex> lock(m);
    return n;
}

} // namespace entwine


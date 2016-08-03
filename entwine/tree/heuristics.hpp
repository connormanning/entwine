/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

namespace entwine
{
namespace heuristics
{

const std::size_t sleepCount(65536 * 20);
const float sparseDepthBumpRatio(1.05);
const double defaultWorkToClipRatio(0.33);
const std::size_t poolBlockSize(1024 * 1024);
const std::size_t clipCacheSize(32);    // Per thread.

} // namespace heuristics
} // namespace entwine


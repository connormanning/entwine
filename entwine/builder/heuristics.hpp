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

#include <cstdint>

namespace entwine
{
namespace heuristics
{

// After this many points (per thread), we'll clip - which involves reference-
// decrementing the chunks that haven't been used in the past two sleepCount
// windows, which will trigger their serialization.
const uint64_t sleepCount(65536 * 32);

// How many unreferenced chunks to keep alive in our chunk cache.
const uint64_t cacheSize(64);

// When building, we are given a total thread count.  Because serialization is
// more expensive than actually doing tree work, we'll allocate more threads to
// the "clip" task than to the "work" task.  This parameter tunes the ratio of
// work threads to clip threads.
const float defaultWorkToClipRatio(0.33f);

// Max number of nodes to store in a single hierarchy file.
const uint64_t maxHierarchyNodesPerFile(32768);

} // namespace heuristics
} // namespace entwine


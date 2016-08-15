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

// After this many points (per thread), we'll clip - which involves reference-
// decrementing the chunks that haven't been used in the past two sleepCount
// windows, which will trigger their serialization.
const std::size_t sleepCount(65536 * 20);

// A per-thread count of the minimum chunk-cache size to keep during clipping.
const std::size_t clipCacheSize(32);

// When building, we are given a total thread count.  Because serialization is
// more expensive than actually doing tree work, we'll allocate more threads to
// the "clip" task than to the "work" task.  This parameter tunes the ratio of
// work threads to clip threads.
const float defaultWorkToClipRatio(0.33);

// These parameters determine how much work to keep when another worker has
// requested to take a portion of our work.
//
// If we're the nominal builder, try to keep a larger portion of work for
// ourselves to minimize the amount of large unsplits.
const float nominalKeepWorkRatio(0.75);
const float defaultKeepWorkRatio(0.50);

// Pooled point cells, data, and hierarchy nodes come from the splice pool,
// which allocates them in blocks.  This sets the block size.
const std::size_t poolBlockSize(1024 * 1024);

// Since hierarchy blocks simply count bucketed points, after the sparse depth
// we don't expect to see much reduction in hierarchy block size - we just
// expect their average magnitudes to decrease.  So keep splitting hierarchy
// blocks well past the point after which we expect the data to get sparse.
const float hierarchySparseFactor(1.25);

} // namespace heuristics
} // namespace entwine


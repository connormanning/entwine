/******************************************************************************
* Copyright (c) 2020, Connor Manning (connor@hobu.co)
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

struct PointCounts
{
    PointCounts(uint64_t inserts = 0, uint64_t outOfBounds = 0)
        : inserts(inserts)
        , outOfBounds(outOfBounds)
    { }

    PointCounts& operator+=(const PointCounts& o)
    {
        inserts += o.inserts;
        outOfBounds += o.outOfBounds;
        return *this;
    }

    uint64_t inserts = 0;
    uint64_t outOfBounds = 0;
};

} // namespace entwine

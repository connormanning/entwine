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

#include <limits>

namespace entwine
{

class Range
{
public:
    Range()
        : min(std::numeric_limits<double>::max())
        , max(std::numeric_limits<double>::lowest())
    { }

    void grow(double val)
    {
        min = std::min(min, val);
        max = std::max(max, val);
    }

    double min;
    double max;
};

} // namespace entwine


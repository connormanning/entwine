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

#include <cstddef>
#include <string>

#include <pdal/PointRef.hpp>

#include <entwine/types/bounds.hpp>

namespace entwine
{

class Filterable
{
public:
    virtual bool check(const pdal::PointRef& pointRef) const = 0;
    virtual bool check(const Bounds& bounds) const { return true; }
    virtual void log(const std::string& pre) const = 0;
};

} // namespace entwine


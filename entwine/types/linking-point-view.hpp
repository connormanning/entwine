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

#include <pdal/PointView.hpp>

#include <entwine/types/simple-point-table.hpp>

namespace entwine
{

class LinkingPointView : public pdal::PointView
{
public:
    LinkingPointView(SimplePointTable& table);
};

} // namespace entwine


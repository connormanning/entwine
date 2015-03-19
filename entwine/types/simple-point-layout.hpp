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

#include <pdal/PointLayout.hpp>

class SimplePointLayout : public pdal::PointLayout
{
private:
    bool update(pdal::Dimension::Detail dimDetail, const std::string& name);
};


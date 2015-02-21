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

#include <pdal/Dimension.hpp>

struct DimInfo
{
    DimInfo(
            const pdal::Dimension::Id::Enum id,
            const pdal::Dimension::Type::Enum type)
        : id(id)
        , type(type)
    { }

    const pdal::Dimension::Id::Enum id;
    const pdal::Dimension::Type::Enum type;

    std::size_t size() const
    {
        return pdal::Dimension::size(type);
    }
};


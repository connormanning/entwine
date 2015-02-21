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

#include <vector>
#include <cstdint>

#include "dim-info.hpp"

struct Schema
{
    explicit Schema(std::vector<DimInfo> dims) : dims(dims) { }
    const std::vector<DimInfo> dims;

    std::size_t stride(bool rasterize = false) const
    {
        std::size_t stride(0);

        for (const auto& dim : dims)
        {
            if (!rasterize || !Schema::rasterOmit(dim.id))
            {
                stride += dim.size();
            }
        }

        if (rasterize)
        {
            // Clientward rasterization schemas always contain a byte to specify
            // whether a point at this location in the raster exists.
            ++stride;
        }

        return stride;
    }

    bool use(const DimInfo& dim, bool rasterize) const
    {
        return !rasterize || !Schema::rasterOmit(dim.id);
    }

    static bool rasterOmit(pdal::Dimension::Id::Enum id)
    {
        // These Dimensions are not explicitly placed in the output buffer
        // for rasterized requests.
        return id == pdal::Dimension::Id::X || id == pdal::Dimension::Id::Y;
    }
};


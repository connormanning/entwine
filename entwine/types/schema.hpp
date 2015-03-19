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
#include <vector>

#include <pdal/PointLayout.hpp>

#include <entwine/third/json/json.h>
#include <entwine/types/dim-info.hpp>

namespace entwine
{

class Schema
{
public:
    explicit Schema(std::vector<DimInfo> dims);
    explicit Schema(const pdal::PointLayoutPtr layout);

    const std::vector<DimInfo>& dims() const;
    const pdal::PointLayoutPtr pdalLayout() const;

    std::size_t pointSize() const;

    Json::Value toJson() const;
    static Schema fromJson(const Json::Value& json);

private:
    std::vector<DimInfo> m_dims;
    const pdal::PointLayoutPtr m_layout;
};

} // namespace entwine


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
    // Populate this schema later.  Call finalize() when done populating the
    // layout.
    Schema();

    // Schema layout will be finalized with these dims.
    explicit Schema(DimList dims);
    Schema(const Json::Value& json);

    ~Schema();

    void finalize();

    const std::vector<DimInfo>& dims() const;
    pdal::PointLayout& pdalLayout() const;

    std::size_t pointSize() const;

    Json::Value toJson() const;

private:
    std::unique_ptr<pdal::PointLayout> m_layout;
    DimList m_dims;
};

} // namespace entwine


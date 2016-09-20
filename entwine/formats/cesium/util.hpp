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

#include <json/json.h>

#include <entwine/types/bounds.hpp>

namespace entwine
{
namespace cesium
{

inline Json::Value boundingVolumeJson(const Bounds& bounds)
{
    Json::Value json;
    Json::Value& box(json["box"]);

    const auto& mid(bounds.mid());
    box.append(mid.x); box.append(mid.y); box.append(mid.z);
    box.append(bounds.width() / 2.0); box.append(0.0); box.append(0.0);
    box.append(0.0); box.append(bounds.depth() / 2.0); box.append(0.0);
    box.append(0.0); box.append(0.0); box.append(bounds.height() / 2.0);

    return json;
}

} // namespace cesium
} // namespace entwine


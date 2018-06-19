/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "entwine.hpp"

#include <entwine/formats/cesium/tileset.hpp>

namespace entwine
{

void App::convert(std::vector<std::string> args)
{
    Json::Value config;
    config["input"] = "~/data/ept/autzen-4978";
    config["output"] = "~/code/cesium-pages/data/convert/cesium";
    cesium::Tileset tileset(config);
    tileset.build();
}

} // namespace entwine



/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <string>

#include <pdal/PipelineManager.hpp>
#include <pdal/Reader.hpp>
#include <pdal/Stage.hpp>

#include <entwine/types/scale-offset.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/optional.hpp>

namespace entwine
{

json::iterator findStage(json& pipeline, std::string type);
json::const_iterator findStage(const json& pipeline, std::string type);

json& findOrAppendStage(json& pipeline, std::string type);
json omitStage(json pipeline, std::string type);

pdal::Stage& getStage(pdal::PipelineManager& pm);
pdal::Reader& getReader(pdal::Stage& last);
pdal::Stage& getFirst(pdal::Stage& last);
json getMetadata(pdal::Reader& reader);
optional<ScaleOffset> getScaleOffset(const pdal::Reader& reader);

} // namespace entwine

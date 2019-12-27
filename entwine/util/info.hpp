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

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/source.hpp>
#include <entwine/util/optional.hpp>

namespace entwine
{

json createInfoPipeline(
    json pipeline = json::array({ json::object() }),
    bool deep = false,
    optional<Reprojection> = { });

json extractInfoPipelineFromConfig(json config);

source::Info analyzeOne(std::string path, bool deep, json pipelineTemplate);
source::Source parseOne(std::string path, const arbiter::Arbiter& a = { });

source::List analyze(
    const json& pipelineTemplate,
    const StringList& inputs,
    bool deep,
    std::string tmp = arbiter::getTempPath(),
    const arbiter::Arbiter& a = { },
    unsigned int threads = 8);

source::List analyze(const json& config);

void serialize(
    const source::List& sources,
    const arbiter::Endpoint& ep,
    unsigned int threads = 8);

} // namespace entwine

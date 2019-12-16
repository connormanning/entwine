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
    optional<Reprojection> = { });

json extractInfoPipelineFromConfig(json config);

source::List analyze(
    const json& pipeline,
    const StringList& inputs,
    const arbiter::Arbiter& a = arbiter::Arbiter(),
    unsigned int threads = 8);

source::List analyze(const json& config);

void serialize(
    const source::List& sources,
    const arbiter::Endpoint& ep,
    unsigned int threads = 8);

} // namespace entwine

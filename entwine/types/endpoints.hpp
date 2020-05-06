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

#include <memory>
#include <stdexcept>
#include <string>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

struct Endpoints
{
    Endpoints(
        std::shared_ptr<arbiter::Arbiter> a,
        std::string output,
        std::string tmp);

    std::shared_ptr<arbiter::Arbiter> arbiter;
    arbiter::Endpoint output;
    arbiter::Endpoint data;
    arbiter::Endpoint hierarchy;
    arbiter::Endpoint sources;
    arbiter::Endpoint tmp;
};

} // namespace entwine

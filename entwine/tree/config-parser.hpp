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

#include <memory>
#include <vector>

#include <entwine/third/json/json.hpp>

namespace arbiter
{
    class Arbiter;
}

namespace entwine
{

class Builder;
class Manifest;

class ConfigParser
{
public:
    static std::unique_ptr<Builder> getBuilder(
            const Json::Value& json,
            std::shared_ptr<arbiter::Arbiter> arbiter,
            std::unique_ptr<Manifest> manifest);

    static std::unique_ptr<Manifest> getManifest(
            const Json::Value& json,
            const arbiter::Arbiter& arbiter);

    static Json::Value parse(const std::string& input);
};

} // namespace entwine


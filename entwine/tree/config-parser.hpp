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

struct RunInfo
{
    RunInfo(std::vector<std::string> manifest, std::size_t maxCount)
        : manifest(manifest)
        , maxCount(maxCount)
    { }

    std::vector<std::string> manifest;
    std::size_t maxCount;
};

class ConfigParser
{
public:
    static std::unique_ptr<Builder> getBuilder(
            const Json::Value& json,
            std::shared_ptr<arbiter::Arbiter> arbiter,
            const RunInfo& runInfo,
            bool force,
            std::pair<std::size_t, std::size_t> subset = { 0, 0 });

    static RunInfo getRunInfo(
            const Json::Value& json,
            const arbiter::Arbiter& arbiter);

    static std::shared_ptr<arbiter::Arbiter> getArbiter(
            std::string credentialsString);

    static Json::Value parse(const std::string& input);
};

} // namespace entwine


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

namespace Json { class Value; }

namespace entwine
{

namespace arbiter { class Arbiter; }

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

private:
    static std::unique_ptr<Builder> tryGetExisting(
            const Json::Value& config,
            const arbiter::Arbiter& arbiter,
            const std::string& outPath,
            const std::string& tmpPath,
            std::size_t numThreads);
};

} // namespace entwine


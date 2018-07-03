/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <memory>
#include <string>
#include <vector>

#include <entwine/third/arbiter/arbiter.hpp>

namespace entwine
{

void ensurePut(
        const arbiter::Endpoint& endpoint,
        const std::string& path,
        const std::vector<char>& data);

inline void ensurePut(
        const arbiter::Endpoint& endpoint,
        const std::string& path,
        const std::string& data)
{
    ensurePut(endpoint, path, std::vector<char>(data.begin(), data.end()));
}

std::unique_ptr<std::vector<char>> ensureGet(
        const arbiter::Endpoint& endpoint,
        const std::string& path);

std::string ensureGetString(
        const arbiter::Endpoint& endpoint,
        const std::string& path);

std::string ensureGet(const arbiter::Arbiter& a, const std::string& path);

} // namespace entwine


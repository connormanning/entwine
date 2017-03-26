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

#include <string>
#include <vector>

namespace arbiter
{
    class Endpoint;
}

namespace entwine
{
namespace io
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

} // namespace io
} // namespace entwine


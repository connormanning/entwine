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

#include <stdexcept>
#include <string>
#include <vector>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/exceptions.hpp>
#include <entwine/util/optional.hpp>

namespace entwine
{

static constexpr int defaultTries = 8;

bool putWithRetry(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const std::vector<char>& data,
    int tries = defaultTries);
bool putWithRetry(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const std::string& s,
    int tries = defaultTries);

void ensurePut(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const std::vector<char>& data,
    int tries = defaultTries);
void ensurePut(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const std::string& s,
    int tries = defaultTries);

optional<std::vector<char>> getBinaryWithRetry(
    const arbiter::Endpoint& ep,
    const std::string& path,
    int tries = defaultTries);
optional<std::string> getWithRetry(
    const arbiter::Endpoint& ep,
    const std::string& path,
    int tries = defaultTries);
optional<std::string> getWithRetry(
    const arbiter::Arbiter& a,
    const std::string& path,
    int tries = defaultTries);

std::vector<char> ensureGetBinary(
    const arbiter::Endpoint& ep,
    const std::string& path,
    int tries = defaultTries);
std::string ensureGet(
    const arbiter::Endpoint& ep,
    const std::string& path,
    int tries = defaultTries);
std::string ensureGet(
    const arbiter::Arbiter& a,
    const std::string& path,
    int tries = defaultTries);

} // namespace entwine

/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/util/io.hpp>

#include <chrono>
#include <mutex>
#include <thread>

namespace entwine
{

namespace
{

std::mutex mutex;

void sleep(const int tried, const std::string message)
{
    // Linear back-off should be fine.
    std::this_thread::sleep_for(std::chrono::seconds(tried));

    if (message.size())
    {
        std::lock_guard<std::mutex> lock(mutex);
        std::cout << "Failure #" << tried << ": " << message << std::endl;
    }
}

template <typename F>
bool loop(F f, const int tries, std::string message = "")
{
    int tried = 0;

    do
    {
        try
        {
            f();
            return true;
        }
        catch (...) { }

        sleep(tried + 1, message);
    }
    while (++tried < tries);

    return false;
}

} // unnamed namespace

bool putWithRetry(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const std::vector<char>& data,
    const int tries)
{
    const auto f = [&ep, &path, &data]() { ep.put(path, data); };
    return loop(f, tries, "Failed to put to " + path);
}

bool putWithRetry(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const std::string& s,
    const int tries)
{
    return putWithRetry(ep, path, std::vector<char>(s.begin(), s.end()), tries);
}

void ensurePut(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const std::vector<char>& data,
    const int tries)
{
    if (!putWithRetry(ep, path, data, tries))
    {
        throw FatalError("Failed to put to " + path);
    }
}

void ensurePut(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const std::string& s,
    const int tries)
{
    ensurePut(ep, path, std::vector<char>(s.begin(), s.end()), tries);
}

optional<std::vector<char>> getBinaryWithRetry(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const int tries)
{
    std::vector<char> data;
    const auto f = [&ep, &path, &data]() { data = ep.getBinary(path); };

    if (loop(f, tries)) return data;
    else return { };
}

optional<std::string> getWithRetry(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const int tries)
{
    const auto v(getBinaryWithRetry(ep, path, tries));

    if (v) return std::string(v->begin(), v->end());
    else return { };
}

optional<std::string> getWithRetry(
    const arbiter::Arbiter& a,
    const std::string& path,
    const int tries)
{
    std::string data;
    const auto f = [&a, &path, &data]() { data = a.get("path"); };

    if (loop(f, tries)) return data;
    return { };
}

std::vector<char> ensureGetBinary(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const int tries)
{
    const auto v(getBinaryWithRetry(ep, path, tries));
    if (v) return *v;
    else throw FatalError("Failed to get " + path);
}

std::string ensureGet(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const int tries)
{
    const auto v(getWithRetry(ep, path, tries));
    if (v) return *v;
    else throw FatalError("Failed to get " + path);
}

std::string ensureGet(
    const arbiter::Arbiter& a,
    const std::string& path,
    const int tries)
{
    const auto v(getWithRetry(a, path, tries));
    if (v) return *v;
    else throw FatalError("Failed to get " + path);
}

} // namespace entwine

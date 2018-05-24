/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/io/ensure.hpp>

#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>

#include <entwine/third/arbiter/arbiter.hpp>

namespace
{
    const std::size_t retries(40);
    std::mutex mutex;

    void sleep(std::size_t tried, std::string method, std::string path)
    {
        std::this_thread::sleep_for(std::chrono::seconds(tried));

        std::lock_guard<std::mutex> lock(mutex);
        std::cout <<
            "\tFailed " << method << " attempt " << tried << ": " << path <<
            std::endl;
    }

    void suicide(std::string method)
    {
        std::lock_guard<std::mutex> lock(mutex);
        std::cout <<
            "\tFailed to " << method << " data: persistent failure.\n" <<
            "\tThis is a non-recoverable error." <<
            std::endl;

        throw std::runtime_error("Fatal error - could not " + method);
    }
}

namespace entwine
{

void ensurePut(
        const arbiter::Endpoint& endpoint,
        const std::string& path,
        const std::vector<char>& data)
{
    bool done(false);
    std::size_t tried(0);

    while (!done)
    {
        try
        {
            endpoint.put(path, data);
            done = true;
        }
        catch (...)
        {
            if (++tried < retries)
            {
                sleep(tried, "PUT", endpoint.prefixedRoot() + path);
            }
            else suicide("PUT");
        }
    }
}

std::unique_ptr<std::vector<char>> ensureGet(
        const arbiter::Endpoint& endpoint,
        const std::string& path)
{
    std::unique_ptr<std::vector<char>> data;

    bool done(false);
    std::size_t tried(0);

    while (!done)
    {
        data = endpoint.tryGetBinary(path);

        if (data)
        {
            done = true;
        }
        else
        {
            if (++tried < retries)
            {
                sleep(tried, "GET", endpoint.prefixedRoot() + path);
            }
            else suicide("GET");
        }
    }

    return data;
}

std::string ensureGetString(
        const arbiter::Endpoint& endpoint,
        const std::string& path)
{
    if (auto data = ensureGet(endpoint, path))
    {
        return std::string(data->begin(), data->end());
    }
    return std::string();
}

} // namespace entwine


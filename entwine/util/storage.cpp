/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/util/storage.hpp>

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
            "\tThis is a non-recoverable error - Abandoning index." <<
            std::endl;

        exit(1);
    }
}

namespace entwine
{

void Storage::ensurePut(
        const arbiter::Endpoint& endpoint,
        const std::string& path,
        const std::vector<char>& data)
{
    bool done(false);
    std::size_t tried(0);

    if (data.empty())
    {
        throw std::runtime_error("Tried to save empty chunk");
    }
    else if (
            data.back() != Chunk::Contiguous &&
            data.back() != Chunk::Sparse)
    {
        throw std::runtime_error("Tried to save improperly marked chunk");
    }

    while (!done)
    {
        try
        {
            endpoint.putSubpath(path, data);
            done = true;
        }
        catch (...)
        {
            if (++tried < retries) sleep(tried, "PUT", path);
            else suicide("PUT");
        }
    }
}

std::unique_ptr<std::vector<char>> Storage::ensureGet(
        const arbiter::Endpoint& endpoint,
        const std::string& path)
{
    std::unique_ptr<std::vector<char>> data;

    bool done(false);
    std::size_t tried(0);

    while (!done)
    {
        data = endpoint.tryGetSubpathBinary(path);

        if (data)
        {
            done = true;
        }
        else
        {
            if (++tried < retries) sleep(tried, "GET", path);
            else suicide("GET");
        }
    }

    return data;
}

} // namespace entwine


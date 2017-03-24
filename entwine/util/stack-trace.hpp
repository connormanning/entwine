/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <csignal>
#include <mutex>

#include <pdal/util/Utils.hpp>

#ifndef _WIN32
#include <execinfo.h>
#include <unistd.h>
#include <dlfcn.h>
#endif

namespace entwine
{

namespace
{
    std::mutex mutex;
}

inline void stackTrace()
{
    std::lock_guard<std::mutex> lock(mutex);

    void* buffer[32];
    const std::size_t size(backtrace(buffer, 32));
    char** symbols(backtrace_symbols(buffer, size));

    std::vector<std::string> lines;

    for (std::size_t i(0); i < size; ++i)
    {
        std::string symbol(symbols[i]);
        Dl_info info;

        if (dladdr(buffer[i], &info))
        {
            const auto demangled(pdal::Utils::demangle(info.dli_sname));

            const std::size_t offset(
                    static_cast<char*>(buffer[i]) -
                    static_cast<char*>(info.dli_saddr));

            // Replace the address and mangled name with a human-readable
            // name.
            std::string prefix(std::to_string(i) + "  ");
            const std::size_t pos(symbol.find("0x"));
            if (pos != std::string::npos)
            {
                prefix = symbol.substr(0, pos);
            }

            lines.push_back(prefix + demangled + " + " +
                    std::to_string(offset));
        }
        else
        {
            lines.push_back(symbol);
        }
    }

    for (const auto& l : lines) std::cout << l << std::endl;

    free(symbols);
}

template<typename Signal>
inline void stackTraceOn(Signal s)
{
#ifndef _WIN32
    signal(s, [](int sig)
    {
        {
            std::lock_guard<std::mutex> lock(mutex);
            std::cout << "Got error " << sig << std::endl;
        }

        stackTrace();
        exit(1);
    });
#endif
}

} // namespace entwine


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

#include <cstdlib>
#include <memory>
#include <string>

#include <entwine/util/unique.hpp>

namespace entwine
{

inline std::unique_ptr<std::string> env(const std::string& var)
{
    std::unique_ptr<std::string> result;

#ifndef _WIN32
    if (const char* c = getenv(var.c_str())) return makeUnique<std::string>(c);
#else
    char* c(nullptr);
    std::size_t size(0);

    if (!_dupenv_s(&c, &size, var.c_str()))
    {
        if (c)
        {
            auto r = makeUnique<std::string>(c);
            free(c);
            return r;
        }
    }
#endif

    return std::unique_ptr<std::string>();
}

} // namespace entwine


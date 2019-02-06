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

#include <algorithm>
#include <cctype>
#include <sstream>
#include <string>

#include <entwine/third/json.hpp>
#include <entwine/types/defs.hpp>

namespace entwine
{

inline std::vector<std::string> keys(const json& j)
{
    std::vector<std::string> result;
    for (const auto& v : j.items()) result.push_back(v.key());
    return result;
}

// Not really JSON-related, but fine for now...
inline std::string commify(const std::size_t n)
{
    std::string s(std::to_string(n));
    for (std::size_t i(s.size() - 3u); i && i < s.size(); i -= 3)
    {
        s.insert(i, ",");
    }

    return s;
}

inline void recMerge(json& dst, const json& add, bool hard = true)
{
    for (const auto& p : add.items())
    {
        const auto& key(p.key());
        const auto& val(p.value());
        if (val.is_object()) recMerge(dst[key], val, hard);
        else if (hard || !dst.count(key)) dst[key] = val;
    }
}

inline json merge(const json& a, const json& b, bool hard = true)
{
    json c(a);
    recMerge(c, b, hard);
    return c;
}

} // namespace entwine


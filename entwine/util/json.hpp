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
#include <cmath>
#include <limits>
#include <sstream>
#include <string>

#include <entwine/third/json.hpp>

namespace entwine
{

inline std::vector<std::string> keys(const json& j)
{
    std::vector<std::string> result;
    for (const auto& v : j.items()) result.push_back(v.key());
    return result;
}

template <typename T>
inline std::vector<T> extractList(const json& j)
{
    if (!j.is_null()) return j.get<std::vector<T>>();
    return { };
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

inline bool isIntegral(double d)
{
    double dummy;
    return std::modf(d, &dummy) == 0;
}

inline json getTypedValue(double d)
{
    if (isIntegral(d))
    {
        if (d < 0) return static_cast<int64_t>(d);
        return static_cast<uint64_t>(d);
    }
    return d;
}

// Slice has the semantics of Javascript's Array.slice, where negative numbers
// indicate an offset from the end of the array.
inline json slice(
    json j,
    int begin = 0,
    int end = std::numeric_limits<int>::max())
{
    if (!j.is_array())
    {
        throw std::runtime_error("Invalid JSON type to slice: " + j.dump(2));
    }

    const int size(static_cast<int>(j.size()));

    if (begin < 0) begin = size + begin;
    if (end < 0) end = size + end;

    begin = std::max(std::min(begin, size), 0);
    end = std::max(std::min(end, size), 0);

    if (begin >= end) return json::array();

    return json(j.begin() + begin, j.begin() + end);
}

} // namespace entwine


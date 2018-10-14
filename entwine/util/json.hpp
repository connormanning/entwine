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

#include <algorithm>
#include <cctype>
#include <sstream>
#include <string>

#include <json/json.h>

#include <entwine/types/defs.hpp>

namespace entwine
{

inline Json::Value parse(const std::string& input)
{
    Json::CharReaderBuilder builder;
    Json::Value json;

    std::istringstream ss(input);
    std::string errors;

    if (input.size())
    {
        if (!parseFromStream(builder, ss, &json, &errors))
        {
            throw std::runtime_error("Error during parsing: " + errors);
        }
    }

    return json;
}

inline Json::Value parse(const char* input)
{
    if (input) return parse(std::string(input));
    else return Json::nullValue;
}

inline Json::Value ensureArray(const Json::Value& in)
{
    if (in.isArray() || in.isNull()) return in;
    else
    {
        Json::Value a(Json::arrayValue);
        a.append(in);
        return a;
    }
}

// If input is not an array or there are no values in the sliced range, a null
// value is returned.
inline Json::Value slice(
        const Json::Value& in,
        const Json::ArrayIndex start,
        const Json::ArrayIndex end =
            std::numeric_limits<Json::ArrayIndex>::max())
{
    Json::Value out;
    if (in.isArray())
    {
        for (Json::ArrayIndex i(start); i < end && i < in.size(); ++i)
        {
            out.append(in[i]);
        }
    }
    return out;
}

// Same as Json::Value::toStyledString but with fixed precision for doubles.
inline std::string toPreciseString(
        const Json::Value& v,
        bool styled = true,
        uint64_t precision = 12,
        uint64_t depth = 0)
{
    const std::string indent(depth, '\t');

    if (v.type() == Json::realValue)
    {
        std::ostringstream oss;
        oss << std::setprecision(precision) << v.asDouble();
        return oss.str();
    }
    else if (v.isObject())
    {
        std::string s;
        s += '{';
        uint64_t i(0);
        for (const std::string key : v.getMemberNames())
        {
            if (i++) s += ',';
            if (styled)
            {
                s += '\n';
                s += indent + '\t';
            }
            s += '"' + key + "\"";
            s += std::string(styled ? " " : "") + ":" + (styled ? " " : "");
            if (styled)
            {
                if (v[key].isObject() || v[key].isArray())
                {
                    s += '\n' + indent + '\t';
                }
            }
            s += toPreciseString(v[key], styled, precision, depth + 1);
        }
        if (styled)
        {
            s += '\n';
            s += indent;
        }
        s += '}';
        return s;
    }
    else if (v.isArray())
    {
        std::string s;
        s += '[';
        for (Json::ArrayIndex i(0); i < v.size(); ++i)
        {
            if (i) s += ',';
            if (styled)
            {
                s += '\n';
                s += indent + '\t';
            }
            s += toPreciseString(v[i], styled, precision, depth + 1);
        }
        if (styled)
        {
            s += '\n';
            s += indent;
        }
        s += ']';
        return s;
    }
    else
    {
        std::ostringstream oss;
        oss << v;
        return oss.str();
    }
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

inline void recMerge(
        Json::Value& dst,
        const Json::Value& add,
        bool hard = true)
{
    for (const auto& key : add.getMemberNames())
    {
        if (add[key].isObject()) recMerge(dst[key], add[key], hard);
        else if (hard || !dst.isMember(key)) dst[key] = add[key];
    }
}

inline Json::Value merge(
        const Json::Value& a,
        const Json::Value& b,
        bool hard = true)
{
    Json::Value c(a);
    recMerge(c, b, hard);
    return c;
}

inline Json::Value merge(
        const Json::Value& a,
        const Json::Value& b,
        const Json::Value& c)
{
    return merge(merge(a, b), c);
}

inline std::string toFastString(const Json::Value& json)
{
    Json::FastWriter writer;
    return writer.write(json);
}

template<typename T>
inline Json::Value toJsonArray(const std::vector<T>& vec)
{
    Json::Value json;
    json.resize(vec.size());

    for (std::size_t i(0); i < vec.size(); ++i)
    {
        json[static_cast<Json::ArrayIndex>(i)] = vec[i];
    }

    return json;
}

template<typename T>
inline Json::Value toJsonArrayOfObjects(const std::vector<T>& vec)
{
    Json::Value json;
    json.resize(vec.size());

    for (std::size_t i(0); i < vec.size(); ++i)
    {
        json[static_cast<Json::ArrayIndex>(i)] = vec[i].toJson();
    }

    return json;
}

namespace extraction
{
    template<typename T, typename F>
    std::vector<T> doExtract(const Json::Value& json, F f)
    {
        std::vector<T> result;

        if (json.isNull() || !json.isArray()) return result;

        result.reserve(json.size());

        for (Json::ArrayIndex i(0); i < json.size(); ++i)
        {
            result.push_back(f(json[i]));
        }

        return result;
    }

    template<typename T, typename Enable = void>
    struct E
    {
        static std::vector<T> go(const Json::Value& json)
        {
            std::vector<T> result;
            result.reserve(json.size());

            for (Json::ArrayIndex i(0); i < json.size(); ++i)
            {
                result.emplace_back(json[i]);
            }

            return result;
        }
    };

    template<typename T>
    struct E<
        T,
        typename std::enable_if<
            std::is_integral<T>::value && std::is_signed<T>::value
        >::type>
    {
        static std::vector<T> go(const Json::Value& json)
        {
            return doExtract<T>(
                    json,
                    [](const Json::Value& v) { return v.asInt64(); });
        }
    };

    template<typename T>
    struct E<
        T,
        typename std::enable_if<
            std::is_integral<T>::value && std::is_unsigned<T>::value
        >::type>
    {
        static std::vector<T> go(const Json::Value& json)
        {
            return doExtract<T>(
                    json,
                    [](const Json::Value& v) { return v.asUInt64(); });
        }
    };

    template<typename T>
    struct E<
        T,
        typename std::enable_if<std::is_floating_point<T>::value>::type>
    {
        static std::vector<T> go(const Json::Value& json)
        {
            return doExtract<T>(
                    json,
                    [](const Json::Value& v) { return v.asDouble(); });
        }
    };

    template<>
    struct E<std::string>
    {
        static std::vector<std::string> go(const Json::Value& json)
        {
            return doExtract<std::string>(
                    json,
                    [](const Json::Value& v) { return v.asString(); });
        }
    };
}

template<typename T>
std::vector<T> extract(const Json::Value& json)
{
    return extraction::E<T>::go(json);
}

} // namespace entwine


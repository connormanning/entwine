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
#include <string>

#include <json/json.h>

#include <entwine/types/defs.hpp>

namespace entwine
{

inline Json::Value parse(const std::string& input)
{
    Json::Value json;
    Json::Reader reader;

    if (input.size())
    {
        if (!reader.parse(input, json, false))
        {
            const std::string jsonError(reader.getFormattedErrorMessages());
            if (!jsonError.empty())
            {
                throw std::runtime_error("Error during parsing: " + jsonError);
            }
        }
    }

    return json;
}

inline void recMerge(Json::Value& dst, const Json::Value& add)
{
    for (const auto& key : add.getMemberNames())
    {
        if (add[key].isObject()) recMerge(dst[key], add[key]);
        else dst[key] = add[key];
    }
}

// Assumptions here are that s contains a quotation-delimited number, possibly
// with whitespace outside of the quotations.
inline Id parseElement(const std::string& s)
{
    std::size_t pos(s.find_first_of('"'));
    const std::size_t end(s.find_last_of('"'));

    if (
            pos == std::string::npos ||
            end == std::string::npos ||
            pos == end)
    {
        throw std::runtime_error("Element is not a string: " + s);
    }

    if (!std::all_of(s.data(), s.data() + pos, ::isspace))
    {
        throw std::runtime_error("Invalid leading characters");
    }
    if (
            end + 1 < s.size() &&
            !std::all_of(s.data() + end + 1, s.data() + s.size(), ::isspace))
    {
        throw std::runtime_error("Invalid trailing characters");
    }

    ++pos;
    return Id(s.substr(pos, end - pos));
}

inline std::vector<Id> extractIds(const std::string& s)
{
    std::vector<Id> ids;
    ids.reserve(std::count(s.begin(), s.end(), ',') + 1);

    std::size_t pos(s.find_first_of('['));
    std::size_t end(0);

    if (
            pos != std::string::npos &&
            !std::all_of(s.data(), s.data() + pos, ::isspace))
    {
        throw std::runtime_error("Invalid characters before opening bracket");
    }

    while (pos != std::string::npos && ++pos < s.size())
    {
        end = s.find_first_of(",]", pos);

        if (end == std::string::npos)
        {
            throw std::runtime_error("Missing token");
        }
        else if (end != pos)
        {
            ids.push_back(parseElement(s.substr(pos, end - pos)));
            pos = s.find_first_of(',', pos);
        }
        else pos = std::string::npos;
    }

    if (ids.size() && s.at(end) != ']')
    {
        throw std::runtime_error("Missing final bracket");
    }
    else if (
            ids.size() &&
            end + 1 < s.size() &&
            !std::all_of(s.data() + end + 1, s.data() + s.size(), ::isspace))
    {
        throw std::runtime_error("Invalid trailing characters");
    }

    return ids;
}

inline std::string toFastString(const Json::Value& json)
{
    Json::FastWriter writer;
    return writer.write(json);
}

namespace extraction
{
    template<typename T, typename F>
    std::vector<T> doExtract(const Json::Value& json, F f)
    {
        std::vector<T> result;
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
            static_assert(!sizeof(T), "Cannot extract JSON as this type");
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

    template<>
    struct E<Id>
    {
        static std::vector<Id> go(const Json::Value& json)
        {
            return doExtract<Id>(
                    json,
                    [](const Json::Value& v) { return Id(v.asString()); });
        }
    };
}

template<typename T>
std::vector<T> extract(const Json::Value& json)
{
    return extraction::E<T>::go(json);
}

} // namespace entwine


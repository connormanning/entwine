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

#include <string>

#include <entwine/third/json/json.hpp>

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
}

template<typename T>
std::vector<T> extract(const Json::Value& json)
{
    return extraction::E<T>::go(json);
}

} // namespace entwine


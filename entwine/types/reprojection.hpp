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

class Reprojection
{
public:
    Reprojection(const std::string in, const std::string out)
        : m_in(in)
        , m_out(out)
    { }

    Reprojection(const Json::Value& json)
        : m_in(json["in"].asString())
        , m_out(json["out"].asString())
    { }

    Json::Value toJson() const
    {
        Json::Value json;
        json["in"] = in();
        json["out"] = out();
        return json;
    }

    std::string in() const
    {
        return m_in;
    }

    std::string out() const
    {
        return m_out;
    }

private:
    std::string m_in;
    std::string m_out;
};

} // namespace entwine


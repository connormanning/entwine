/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/reprojection.hpp>

#include <entwine/third/json/json.h>

namespace entwine
{

Reprojection::Reprojection(const std::string in, const std::string out)
    : m_in(in)
    , m_out(out)
{ }

Reprojection::Reprojection(const Json::Value& json)
    : m_in(json["in"].asString())
    , m_out(json["out"].asString())
{ }

Json::Value Reprojection::toJson() const
{
    Json::Value json;
    json["in"] = in();
    json["out"] = out();
    return json;
}

std::string Reprojection::in() const
{
    return m_in;
}

std::string Reprojection::out() const
{
    return m_out;
}

} // namespace entwine


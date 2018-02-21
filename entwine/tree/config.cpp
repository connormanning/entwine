/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/config.hpp>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

Config::Config(const Json::Value& json)
    : m_json(merge(defaults(), json))
{ }

Json::Value Config::defaults() const
{
    Json::Value json;
    json["input"] = Json::Value::null;
    json["output"] = Json::Value::null;
    json["tmp"] = arbiter::fs::getTempPath();
    json["threads"] = 8;
    json["trustHeaders"] = true;
    json["head"] = 7;
    json["body"] = 9;
    json["dataStorage"] = "laszip";
    json["hierStorage"] = "json";
    return json;
}

} // namespace entwine


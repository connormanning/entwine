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
#include <entwine/tree/inference.hpp>
#include <entwine/types/file-info.hpp>
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

void Config::finalize()
{
    if (m_json["input"].isString()) infer();
}

void Config::infer()
{
    // TODO Pass options.
    Inference inference(m_json["input"].asString());
    inference.go();

    // TODO Don't overwrite parameters that have been manually specified.
    m_json["schema"] = inference.schema().toJson();
    m_json["bounds"] = inference.bounds().toJson();
    m_json["input"] = toJson(inference.fileInfo());
    if (const Delta* d = inference.delta())
    {
        inference.delta()->insertInto(m_json);
    }
}

} // namespace entwine


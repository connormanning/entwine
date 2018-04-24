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
#include <entwine/util/json.hpp>

namespace entwine
{

Config::Config(const Json::Value& json)
    : m_json(merge(defaults(), json))
{ }

Json::Value Config::defaults() const
{
    Json::Value json;
    json["tmp"] = arbiter::fs::getTempPath();
    json["threads"] = 8;
    json["trustHeaders"] = true;
    json["structure"]["head"] = 8;
    json["structure"]["body"] = 8;
    json["structure"]["tail"] = 11;
    json["dataStorage"] = "laszip";
    json["hierarchyStorage"] = "json";
    return json;
}

Config Config::prepare()
{
    NewInference inference(*this);
    return inference.go();
}

FileInfoList Config::input() const
{
    FileInfoList f;
    arbiter::Arbiter arbiter(m_json["arbiter"]);

    auto insert([&](const Json::Value& j)
    {
        if (j.isObject())
        {
            f.emplace_back(j);
            return;
        }

        if (!j.isString())
        {
            throw std::runtime_error(
                    j.toStyledString() + " not convertible to string");
        }

        std::string p(j.asString());

        if (p.empty()) return;

        if (p.back() != '*')
        {
            if (arbiter::util::isDirectory(p)) p += '*';
            else if (
                    arbiter::util::getBasename(p).find_first_of('.') ==
                    std::string::npos)
            {
                p += "/*";
            }
        }

        Paths current(arbiter.resolve(p, true));
        std::sort(current.begin(), current.end());
        for (const auto& c : current) f.emplace_back(c);
    });

    const auto& i(m_json["input"]);
    if (i.isString()) insert(i);
    else if (i.isArray()) for (const auto& j : i) insert(j);

    return f;
}

} // namespace entwine


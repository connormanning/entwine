/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/manifest.hpp>

#include <limits>

namespace entwine
{

Origin Manifest::invalidOrigin()
{
    return std::numeric_limits<Origin>::max();
}

Manifest::Manifest()
    : m_originList()
    , m_missedList()
    , m_reverseLookup()
{ }

Manifest::Manifest(const Json::Value& json)
    : m_originList()
    , m_missedList()
    , m_reverseLookup()
{
    const Json::Value& originJson(json["input"]);
    const Json::Value& missedJson(json["missed"]);

    for (Json::ArrayIndex i(0); i < originJson.size(); ++i)
    {
        m_originList.push_back(originJson[i].asString());
        m_reverseLookup.insert(originJson[i].asString());
    }

    for (Json::ArrayIndex i(0); i < missedJson.size(); ++i)
    {
        m_missedList.push_back(missedJson[i].asString());
    }
}

Json::Value Manifest::getJson() const
{
    Json::Value json;

    Json::Value& originJson(json["input"]);
    Json::Value& missedJson(json["missed"]);

    for (Json::ArrayIndex i(0); i < m_originList.size(); ++i)
    {
        originJson.append(m_originList[i]);
    }

    for (Json::ArrayIndex i(0); i < m_missedList.size(); ++i)
    {
        missedJson.append(m_missedList[i]);
    }

    return json;
}

Origin Manifest::addOrigin(const std::string& path)
{
    Origin origin(Manifest::invalidOrigin());

    if (!m_reverseLookup.count(path))
    {
        origin = m_originList.size();

        m_originList.push_back(path);
        m_reverseLookup.insert(path);
    }

    return origin;
}

void Manifest::addOmission(const std::string& path)
{
    m_missedList.push_back(path);
}

} // namespace entwine


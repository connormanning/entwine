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

#include <iostream>
#include <limits>

namespace entwine
{

Origin Manifest::invalidOrigin()
{
    return std::numeric_limits<Origin>::max();
}

Manifest::Manifest()
    : m_originList()
    , m_omissionList()
    , m_errorList()
    , m_originCount(0)
    , m_omissionCount(0)
    , m_errorCount(0)
    , m_reverseLookup()
{ }

Manifest::Manifest(const Json::Value& json)
    : m_originList()
    , m_omissionList()
    , m_errorList()
    , m_originCount(0)
    , m_omissionCount(0)
    , m_errorCount(0)
    , m_reverseLookup()
{
    const Json::Value& originJson(json["input"]);
    const Json::Value& omissionJson(json["omissions"]);
    const Json::Value& errorJson(json["errors"]);

    for (Json::ArrayIndex i(0); i < originJson.size(); ++i)
    {
        m_originList.push_back(originJson[i].asString());
        m_reverseLookup.insert(originJson[i].asString());
    }

    for (Json::ArrayIndex i(0); i < omissionJson.size(); ++i)
    {
        m_omissionList.push_back(omissionJson[i].asString());
    }

    for (Json::ArrayIndex i(0); i < errorJson.size(); ++i)
    {
        m_errorList.push_back(errorJson[i].asUInt64());
    }

    m_originCount = m_originList.size();
    m_omissionCount = m_omissionList.size();
    m_errorCount = m_errorList.size();
}

Json::Value Manifest::toJson() const
{
    Json::Value json;

    Json::Value& originJson(json["input"]);
    Json::Value& omissionJson(json["omissions"]);
    Json::Value& errorJson(json["errors"]);

    for (Json::ArrayIndex i(0); i < m_originList.size(); ++i)
    {
        originJson.append(m_originList[i]);
    }

    for (Json::ArrayIndex i(0); i < m_omissionList.size(); ++i)
    {
        omissionJson.append(m_omissionList[i]);
    }

    for (Json::ArrayIndex i(0); i < m_errorList.size(); ++i)
    {
        errorJson.append(static_cast<Json::UInt64>(m_errorList[i]));
    }

    return json;
}

Json::Value Manifest::jsonCounts() const
{
    Json::Value json;

    json["numInserted"] = static_cast<Json::UInt64>(m_originCount.load());
    json["numOmissions"] = static_cast<Json::UInt64>(m_omissionCount.load());
    json["numErrors"] = static_cast<Json::UInt64>(m_errorCount.load());

    return json;
}

Origin Manifest::addOrigin(const std::string& path)
{
    Origin origin(Manifest::invalidOrigin());

    if (!m_reverseLookup.count(path))
    {
        origin = m_originList.size();

        ++m_originCount;
        m_originList.push_back(path);

        m_reverseLookup.insert(path);
    }

    return origin;
}

void Manifest::addOmission(const std::string& path)
{
    ++m_omissionCount;
    m_omissionList.push_back(path);
}

void Manifest::addError(const Origin origin)
{
    std::cout << "Got error at origin: " << origin << std::endl;
    ++m_errorCount;
    m_errorList.push_back(origin);
}

} // namespace entwine


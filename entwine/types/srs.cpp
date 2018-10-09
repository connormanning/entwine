/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/srs.hpp>

#include <regex>

namespace entwine
{

namespace
{
    const std::regex authCodeRegex("(\\w+):(\\d+)(\\+\\d+)?");
}

Srs::Srs(const std::string full)
    : m_spatialReference(full)
    , m_wkt(m_spatialReference.getWKT())
{
    std::smatch match;
    if (std::regex_match(full, match, authCodeRegex))
    {
        m_authority = match[1];
        m_horizontal = match[2];
        if (match.size() == 4 && match[3].str().size())
        {
            // Remove the leading '+'.
            m_vertical = match[3].str().substr(1);
        }
    }

    if (m_horizontal.empty())
    {
        m_horizontal = m_spatialReference.identifyHorizontalEPSG();
        if (m_horizontal.size() && m_authority.empty()) m_authority = "EPSG";
    }

    if (m_vertical.empty())
    {
        m_vertical = m_spatialReference.identifyVerticalEPSG();
        if (m_vertical.size() && m_authority.empty()) m_authority = "EPSG";
    }
}

Srs::Srs(const Json::Value& json)
{
    if (json.isString())
    {
        throw std::runtime_error("Invalid SRS JSON - must be object");
    }

    m_authority = json["authority"].asString();
    m_horizontal = json["horizontal"].asString();
    m_vertical = json["vertical"].asString();

    if (json.isMember("wkt"))
    {
        m_spatialReference.set(json["wkt"].asString());
        m_wkt = json["wkt"].asString();
    }
}

Json::Value Srs::toJson() const
{
    Json::Value json(Json::objectValue);
    if (m_authority.size()) json["authority"] = m_authority;
    if (m_horizontal.size()) json["horizontal"] = m_horizontal;
    if (m_vertical.size()) json["vertical"] = m_vertical;
    if (m_wkt.size()) json["wkt"] = m_wkt;
    return json;
}

std::string Srs::codeString() const
{
    if (!hasCode()) throw std::runtime_error("No SRS code found");
    std::string s(authority() + ':' + horizontal());
    if (hasVerticalCode()) s += '+' + vertical();
    return s;
}

} // namespace entwine


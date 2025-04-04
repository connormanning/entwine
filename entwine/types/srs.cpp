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

namespace entwine
{

namespace
{

const auto isint = [](const std::string s)
{
    return s.find_first_not_of("0123456789") == std::string::npos;
};

} // unnamed namespace

Srs::Srs(std::string full)
    : m_spatialReference(full)
    , m_wkt(m_spatialReference.getWKT2())
{
    auto pos = full.find(':');
    if (pos != std::string::npos)
    {
        m_authority = full.substr(0, pos);
        std::string code = full.substr(pos + 1);
        pos = code.find('+');
        if (pos != std::string::npos)
        {
            m_horizontal = code.substr(0, pos);
            m_vertical = code.substr(pos + 1);
        }
        else m_horizontal = code;

        // Make sure that horizontal and vertical are digits
        if (!isint(m_horizontal)) m_horizontal.clear();
        if (!isint(m_vertical)) m_vertical.clear();
    }

    // If we were passed WKT instead of a code string, see if we can identify
    // the corresponding codes from the pdal::SpatialReference.

    if (m_horizontal.empty())
    {
        m_horizontal = m_spatialReference.identifyHorizontalEPSG();
        if (m_horizontal.size() && m_authority.empty()) m_authority = "EPSG";
    }

    // Vertical should be populated iff horizontal is populated.
    if (!m_horizontal.empty() && m_vertical.empty())
    {
        m_vertical = m_spatialReference.identifyVerticalEPSG();
        if (m_vertical.size() && m_authority.empty()) m_authority = "EPSG";
    }
}

Srs::Srs(const json& j)
{
    if (j.is_null()) return;

    if (j.is_string())
    {
        *this = Srs(j.get<std::string>());
        return;
    }

    m_authority = j.value("authority", "");
    m_horizontal = j.value("horizontal", "");
    m_vertical = j.value("vertical", "");

    // preferentially set from wkt2
    if (j.count("wkt2"))
    {
        m_wkt = j.at("wkt2").get<std::string>();
        m_spatialReference.set(m_wkt);
    }

    if (j.count("wkt"))
    {
        m_wkt = j.at("wkt").get<std::string>();
        m_spatialReference.set(m_wkt);
    }
}

void to_json(json& j, const Srs& srs)
{
    j = json::object();
    if (srs.authority().size()) j["authority"] = srs.authority();
    if (srs.horizontal().size()) j["horizontal"] = srs.horizontal();
    if (srs.vertical().size()) j["vertical"] = srs.vertical();
    if (srs.wkt().size()) j["wkt"] = srs.wkt();
    std::string wkt2 = srs.wkt2();
    if (wkt2.size()) j["wkt2"] = srs.wkt2();
}

void from_json(const json& j, Srs& srs) { srs = Srs(j); }

std::string Srs::toString() const
{
    if (hasCode()) return codeString();
    return m_wkt;
}

std::string Srs::codeString() const
{
    if (!hasCode()) throw std::runtime_error("No SRS code found");
    std::string s(authority() + ':' + horizontal());
    if (hasVerticalCode()) s += '+' + vertical();
    return s;
}

bool operator==(const Srs& a, const Srs& b)
{
    if (a.hasCode() && b.hasCode()) return a.codeString() == b.codeString();
    return a.wkt() == b.wkt();
}

bool operator!=(const Srs& a, const Srs& b) { return !(a == b); }

} // namespace entwine

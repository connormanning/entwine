/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <string>

#include <pdal/SpatialReference.hpp>

#include <entwine/util/json.hpp>

namespace entwine
{

class Srs
{
public:
    Srs() { }

    // Construct from string, which may be WKT or a code of the format
    // Authority:Horizontal or Authority:Horizontal+Vertical.
    //
    // If the string is a code, we'll pull authority/horizontal/vertical values
    // directly from the string.  Otherwise, we'll try to identify these values
    // with the pdal::SpatialReference::identifyEPSG functions.
    Srs(std::string s);
    Srs(const char* c) : Srs(std::string(c)) { }
    Srs(const Srs& other) : Srs(other.wkt()) { }

    // Construct from JSON.  In this case we won't do any inference - just pluck
    // out previously determined values.
    Srs(const json& j);

    void clear() { *this = Srs(); }

    bool empty() const { return m_spatialReference.empty(); }
    bool exists() const { return !empty(); }
    bool hasCode() const
    {
        return !m_authority.empty() && !m_horizontal.empty();
    }
    bool hasVerticalCode() const { return !m_vertical.empty(); }
    std::string toString() const;
    std::string codeString() const;

    const pdal::SpatialReference& ref() const { return m_spatialReference; }
    const std::string& authority() const { return m_authority; }
    const std::string& horizontal() const { return m_horizontal; }
    const std::string& vertical() const { return m_vertical; }
    const std::string& wkt() const { return m_wkt; }
    const std::string wkt2() const { return m_spatialReference.getWKT2(); }
    const std::string projjson() const { return m_spatialReference.getPROJJSON(); }

private:
    pdal::SpatialReference m_spatialReference;

    std::string m_authority;
    std::string m_horizontal;
    std::string m_vertical;
    std::string m_wkt;
    std::string m_wkt2;
};

bool operator==(const Srs& a, const Srs& b);
bool operator!=(const Srs& a, const Srs& b);

void to_json(json& j, const Srs& srs);
void from_json(const json& j, Srs& srs);

} // namespace entwine


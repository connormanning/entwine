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

    // Construct from JSON.  In this case we won't do any inference - just pluck
    // out previously determined values.
    Srs(const Json::Value& json);

    void clear() { *this = Srs(); }

    Json::Value toJson() const;

    bool empty() const { return m_spatialReference.empty(); }
    bool exists() const { return !empty(); }
    bool hasCode() const { return !m_authority.empty(); }
    bool hasVerticalCode() const { return !m_vertical.empty(); }
    std::string codeString() const;

    const pdal::SpatialReference& ref() const { return m_spatialReference; }
    const std::string& authority() const { return m_authority; }
    const std::string& horizontal() const { return m_horizontal; }
    const std::string& vertical() const { return m_vertical; }
    const std::string& wkt() const { return m_wkt; }

private:
    pdal::SpatialReference m_spatialReference;

    std::string m_authority;
    std::string m_horizontal;
    std::string m_vertical;
    std::string m_wkt;
};

} // namespace entwine


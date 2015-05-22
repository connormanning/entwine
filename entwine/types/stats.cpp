/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/stats.hpp>

namespace entwine
{

Stats::Stats()
    : m_numPoints(0)
    , m_numOutOfBounds(0)
    , m_numFallThroughs(0)
{ }

Stats::Stats(const Stats& other)
    : m_numPoints(other.getNumPoints())
    , m_numOutOfBounds(other.getNumOutOfBounds())
    , m_numFallThroughs(other.getNumFallThroughs())
{ }

Stats::Stats(const Json::Value& json)
    : m_numPoints(json["numPoints"].asUInt64())
    , m_numOutOfBounds(json["numOutOfBounds"].asUInt64())
    , m_numFallThroughs(json["numFallThroughs"].asUInt64())
{ }

Stats& Stats::operator=(const Stats& other)
{
    m_numPoints = other.getNumPoints();
    m_numOutOfBounds = other.getNumOutOfBounds();
    m_numFallThroughs = other.getNumFallThroughs();
    return *this;
}

void Stats::addPoint()
{
    m_numPoints.fetch_add(1);
}

void Stats::addOutOfBounds()
{
    m_numOutOfBounds.fetch_add(1);
}

void Stats::addFallThrough()
{
    m_numFallThroughs.fetch_add(1);
}

std::size_t Stats::getNumPoints() const
{
    return m_numPoints.load();
}

std::size_t Stats::getNumOutOfBounds() const
{
    return m_numOutOfBounds.load();
}

std::size_t Stats::getNumFallThroughs() const
{
    return m_numFallThroughs.load();
}

Json::Value Stats::toJson() const
{
    Json::Value json;
    json["numPoints"] = static_cast<Json::UInt64>(getNumPoints());
    json["numOutOfBounds"] = static_cast<Json::UInt64>(getNumOutOfBounds());
    json["numFallThroughs"] = static_cast<Json::UInt64>(getNumFallThroughs());
    return json;
}

} // namespace entwine


/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>

#include <json/json.h>

#include <entwine/types/defs.hpp>

namespace entwine
{

class PointStats
{
public:
    PointStats() = default;

    explicit PointStats(const Json::Value& json)
        : m_inserts(json["inserts"].asUInt64())
        , m_outOfBounds(json["outOfBounds"].asUInt64())
        , m_overflows(json["overflows"].asUInt64())
    { }

    Json::Value toJson() const
    {
        Json::Value json;
        json["inserts"] = static_cast<Json::UInt64>(m_inserts);
        json["outOfBounds"] = static_cast<Json::UInt64>(m_outOfBounds);
        json["overflows"] = static_cast<Json::UInt64>(m_overflows);
        return json;
    }

    void add(const PointStats& other)
    {
        m_inserts += other.m_inserts;
        m_outOfBounds += other.m_outOfBounds;
        m_overflows += other.m_overflows;
    }

    void addInsert()        { ++m_inserts; }
    void addOutOfBounds()   { ++m_outOfBounds; }
    void addOverflow()      { ++m_overflows; }

    std::size_t inserts() const     { return m_inserts; }
    std::size_t outOfBounds() const { return m_outOfBounds; }
    std::size_t overflows() const   { return m_overflows; }

    void addOutOfBounds(std::size_t n) { m_outOfBounds += n; }
    void clear()
    {
        m_inserts = 0;
        m_outOfBounds = 0;
        m_overflows = 0;
    }

private:
    std::size_t m_inserts = 0;
    std::size_t m_outOfBounds = 0;
    std::size_t m_overflows = 0;
};

class FileStats
{
public:
    FileStats() = default;

    explicit FileStats(const Json::Value& json)
        : m_inserts(json["inserts"].asUInt64())
        , m_omits(json["omits"].asUInt64())
        , m_errors(json["errors"].asUInt64())
    { }

    Json::Value toJson() const
    {
        Json::Value json;
        json["inserts"] = static_cast<Json::UInt64>(m_inserts);
        json["omits"] = static_cast<Json::UInt64>(m_omits);
        json["errors"] = static_cast<Json::UInt64>(m_errors);
        return json;
    }

    void add(const FileStats& other)
    {
        m_inserts += other.m_inserts;
        m_omits += other.m_omits;
        m_errors += other.m_errors;
    }

    void addInsert()    { ++m_inserts; }
    void addOmit()      { ++m_omits; }
    void addError()     { ++m_errors; }

private:
    std::size_t m_inserts = 0;
    std::size_t m_omits = 0;
    std::size_t m_errors = 0;
};

typedef std::map<Origin, PointStats> PointStatsMap;

} // namespace entwine


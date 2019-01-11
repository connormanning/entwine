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

#include <entwine/types/defs.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

class PointStats
{
public:
    PointStats() = default;

    PointStats(uint64_t inserts, uint64_t outOfBounds)
        : m_inserts(inserts)
        , m_outOfBounds(outOfBounds)
    { }

    bool empty() const { return !m_inserts && !m_outOfBounds; }

    PointStats& operator+=(const PointStats& other)
    {
        add(other);
        return *this;
    }

    void add(const PointStats& other)
    {
        m_inserts += other.m_inserts;
        m_outOfBounds += other.m_outOfBounds;
    }

    void addInsert()        { ++m_inserts; }
    void addOutOfBounds()   { ++m_outOfBounds; }

    std::size_t inserts() const     { return m_inserts; }
    std::size_t outOfBounds() const { return m_outOfBounds; }
    std::size_t oob() const { return outOfBounds(); }

    void addOutOfBounds(std::size_t n) { m_outOfBounds += n; }
    void clear()
    {
        m_inserts = 0;
        m_outOfBounds = 0;
    }

private:
    std::size_t m_inserts = 0;
    std::size_t m_outOfBounds = 0;
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

    bool empty() const { return !m_inserts && !m_omits && !m_errors; }

private:
    std::size_t m_inserts = 0;
    std::size_t m_omits = 0;
    std::size_t m_errors = 0;
};

typedef std::map<Origin, PointStats> PointStatsMap;

} // namespace entwine


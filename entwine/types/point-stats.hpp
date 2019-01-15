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

typedef std::map<Origin, PointStats> PointStatsMap;

} // namespace entwine


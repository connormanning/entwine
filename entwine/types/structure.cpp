/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/structure.hpp>

namespace entwine
{

Structure::Structure(
        const std::size_t nullDepth,
        const std::size_t baseDepth,
        const std::size_t coldDepth,
        const std::size_t chunkPoints,
        const std::size_t dimensions)
    : m_nullDepth(nullDepth)
    , m_baseDepth(0)
    , m_coldDepth(0)
    , m_nullIndexBegin(0)
    , m_nullIndexEnd(0)
    , m_baseIndexBegin(0)
    , m_baseIndexEnd(0)
    , m_coldIndexBegin(0)
    , m_coldIndexEnd(0)
    , m_chunkPoints(chunkPoints)
    , m_dimensions(dimensions)
{
    if (m_dimensions != 2)
    {
        throw std::runtime_error("Only quadtree supported now");
    }

    // Ensure ascending order.
    m_baseDepth = std::max(m_nullDepth, baseDepth);
    m_coldDepth = std::max(m_baseDepth, coldDepth);

    loadIndexValues();
}

Structure::Structure(const Json::Value& json)
    : m_nullDepth(json["nullDepth"].asUInt64())
    , m_baseDepth(json["baseDepth"].asUInt64())
    , m_coldDepth(json["coldDepth"].asUInt64())
    , m_nullIndexBegin(0)
    , m_nullIndexEnd(0)
    , m_baseIndexBegin(0)
    , m_baseIndexEnd(0)
    , m_coldIndexBegin(0)
    , m_coldIndexEnd(0)
    , m_chunkPoints(json["chunkPoints"].asUInt64())
    , m_dimensions(json["dimensions"].asUInt64())
{
    loadIndexValues();
}

void Structure::loadIndexValues()
{
    m_nullIndexBegin = 0;
    m_nullIndexEnd = calcOffset(m_nullDepth, m_dimensions);
    m_baseIndexBegin = m_nullIndexEnd;
    m_baseIndexEnd = calcOffset(m_baseDepth, m_dimensions);
    m_coldIndexBegin = m_baseIndexEnd;
    m_coldIndexEnd = calcOffset(m_coldDepth, m_dimensions);

    if (coldIndexSpan())
    {
        if (!m_chunkPoints || (coldIndexSpan() % m_chunkPoints))
        {
            throw std::runtime_error("Invalid chunk specification");
        }
    }
}

Json::Value Structure::toJson() const
{
    Json::Value json;

    json["nullDepth"] = static_cast<Json::UInt64>(nullDepth());
    json["baseDepth"] = static_cast<Json::UInt64>(baseDepth());
    json["coldDepth"] = static_cast<Json::UInt64>(coldDepth());
    json["chunkPoints"] = static_cast<Json::UInt64>(chunkPoints());
    json["dimensions"] = static_cast<Json::UInt64>(dimensions());

    return json;
}

std::size_t Structure::calcOffset(std::size_t depth, std::size_t dimensions)
{
    std::size_t offset(0);

    for (std::size_t i(0); i < depth; ++i)
    {
        offset = (offset << dimensions) + 1;
    }

    return offset;
}

std::size_t Structure::nullDepth() const    { return m_nullDepth; }
std::size_t Structure::baseDepth() const    { return m_baseDepth; }
std::size_t Structure::coldDepth() const    { return m_coldDepth; }

std::size_t Structure::nullIndexBegin() const   { return m_nullIndexBegin; }
std::size_t Structure::nullIndexEnd() const     { return m_nullIndexEnd; }
std::size_t Structure::baseIndexBegin() const   { return m_baseIndexBegin; }
std::size_t Structure::baseIndexEnd() const     { return m_baseIndexEnd; }
std::size_t Structure::coldIndexBegin() const   { return m_coldIndexBegin; }
std::size_t Structure::coldIndexEnd() const     { return m_coldIndexEnd; }

std::size_t Structure::nullIndexSpan() const
{
    return m_nullIndexEnd - m_nullIndexBegin;
}

std::size_t Structure::baseIndexSpan() const
{
    return m_baseIndexEnd - m_baseIndexBegin;
}

std::size_t Structure::coldIndexSpan() const
{
    return m_coldIndexEnd - m_coldIndexBegin;
}

bool Structure::isWithinNull(const std::size_t index) const
{
    return index >= m_nullIndexBegin && index < m_nullIndexEnd;
}

bool Structure::isWithinBase(const std::size_t index) const
{
    return index >= m_baseIndexBegin && index < m_baseIndexEnd;
}

bool Structure::isWithinCold(const std::size_t index) const
{
    return index >= m_coldIndexBegin && index < m_coldIndexEnd;
}

bool Structure::inRange(const std::size_t index) const
{
    return index < m_coldIndexEnd;
}

std::size_t Structure::chunkPoints() const
{
    return m_chunkPoints;
}

std::size_t Structure::dimensions() const
{
    return m_dimensions;
}

} // namespace entwine


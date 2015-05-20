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

#include <entwine/third/json/json.h>

namespace entwine
{

class Structure
{
public:
    Structure(
            std::size_t nullDepth,
            std::size_t baseDepth,
            std::size_t coldDepth,
            std::size_t chunkPoints,
            std::size_t dimensions);

    Structure(const Json::Value& json);

    Json::Value toJson() const;

    static std::size_t calcOffset(std::size_t depth, std::size_t dimensions);

    std::size_t nullDepth() const;
    std::size_t baseDepth() const;
    std::size_t coldDepth() const;

    std::size_t nullIndexBegin() const;
    std::size_t nullIndexEnd() const;
    std::size_t baseIndexBegin() const;
    std::size_t baseIndexEnd() const;
    std::size_t coldIndexBegin() const;
    std::size_t coldIndexEnd() const;

    std::size_t nullIndexSpan() const;
    std::size_t baseIndexSpan() const;
    std::size_t coldIndexSpan() const;

    bool isWithinNull(std::size_t index) const;
    bool isWithinBase(std::size_t index) const;
    bool isWithinCold(std::size_t index) const;
    bool inRange(std::size_t index) const;

    std::size_t chunkPoints() const;
    std::size_t dimensions() const;

private:
    void loadIndexValues();

    std::size_t m_nullDepth;
    std::size_t m_baseDepth;
    std::size_t m_coldDepth;

    std::size_t m_nullIndexBegin;
    std::size_t m_nullIndexEnd;
    std::size_t m_baseIndexBegin;
    std::size_t m_baseIndexEnd;
    std::size_t m_coldIndexBegin;
    std::size_t m_coldIndexEnd;

    std::size_t m_chunkPoints;
    std::size_t m_dimensions;
};

} // namespace entwine


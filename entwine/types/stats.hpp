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

#include <atomic>

#include <entwine/third/json/json.hpp>

namespace entwine
{

class Stats
{
public:
    Stats();
    Stats(const Stats& other);
    explicit Stats(const Json::Value& json);
    Stats& operator=(const Stats& other);

    void addPoint(std::size_t inc = 1);
    void addOutOfBounds(std::size_t inc = 1);
    void addFallThrough(std::size_t inc = 1);

    std::size_t getNumPoints() const;
    std::size_t getNumOutOfBounds() const;
    std::size_t getNumFallThroughs() const;

    Json::Value toJson() const;

private:
    std::atomic_size_t m_numPoints;
    std::atomic_size_t m_numOutOfBounds;
    std::atomic_size_t m_numFallThroughs;
};

} // namespace entwine


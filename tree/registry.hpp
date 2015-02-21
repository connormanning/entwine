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

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "types/elastic-atomic.hpp"
#include "types/point.hpp"

class BBox;
struct PointInfo;
class Roller;

typedef std::vector<std::pair<uint64_t, std::size_t>> MultiResults;

// Maintains mapping to house the data belonging to each virtual node.
class Registry
{
public:
    Registry(
            std::size_t pointSize,
            std::size_t baseDepth = 12, // TODO - Set.
            std::size_t flatDepth = 12,
            std::size_t deadDepth = 12);
    Registry(
            std::size_t pointSize,
            std::shared_ptr<std::vector<char>> data,
            std::size_t baseDepth = 12, // TODO - Set.
            std::size_t flatDepth = 12,
            std::size_t deadDepth = 12);
    ~Registry();

    void put(PointInfo** toAddPtr, Roller& roller);

    void getPoints(
            const Roller& roller,
            MultiResults& results,
            std::size_t depthBegin,
            std::size_t depthEnd);

    void getPoints(
            const Roller& roller,
            MultiResults& results,
            const BBox& query,
            std::size_t depthBegin,
            std::size_t depthEnd);

    std::shared_ptr<std::vector<char>> baseData() { return m_baseData; }

private:
    const std::size_t m_pointSize;

    const std::size_t m_baseDepth;
    const std::size_t m_flatDepth;
    const std::size_t m_deadDepth;

    const std::size_t m_baseOffset;
    const std::size_t m_flatOffset;
    const std::size_t m_deadOffset;

    // TODO Real structures.  These are testing only.
    std::vector<ElasticAtomic<const Point*>> m_basePoints;
    std::shared_ptr<std::vector<char>> m_baseData;
    std::vector<std::mutex> m_baseLocks;
};


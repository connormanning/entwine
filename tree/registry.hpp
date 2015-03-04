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
            std::size_t baseDepth,
            std::size_t flatDepth,
            std::size_t diskDepth);
    Registry(
            std::size_t pointSize,
            std::vector<char>* data,
            std::size_t baseDepth,
            std::size_t flatDepth,
            std::size_t diskDepth);
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

    std::vector<char>& baseData() { return *m_baseData.get(); }

    std::size_t baseDepth() const { return m_baseDepth; }
    std::size_t flatDepth() const { return m_flatDepth; }
    std::size_t diskDepth() const { return m_diskDepth; }

private:
    const std::size_t m_pointSize;

    const std::size_t m_baseDepth;
    const std::size_t m_flatDepth;
    const std::size_t m_diskDepth;

    const std::size_t m_baseOffset;
    const std::size_t m_flatOffset;
    const std::size_t m_diskOffset;

    // TODO Real structures.  These are testing only.
    std::vector<ElasticAtomic<const Point*>> m_basePoints;
    std::unique_ptr<std::vector<char>> m_baseData;
    std::vector<std::mutex> m_baseLocks;
};


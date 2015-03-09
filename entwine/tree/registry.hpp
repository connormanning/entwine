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

namespace Json
{
    class Value;
}

namespace entwine
{

class BBox;
class BaseBranch;
class Branch;
class PointInfo;
class Roller;
class Schema;

typedef std::vector<std::size_t> MultiResults;

// Maintains mapping to house the data belonging to each virtual node.
class Registry
{
public:
    Registry(
            const Schema& schema,
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

    void save(const std::string& dir, Json::Value& meta) const;
    void load(const std::string& dir, const Json::Value& meta);

    // TODO
    // getPointData(std::size_t index);

private:
    Branch* getBranch(const Roller& roller) const;

    const std::size_t m_baseDepth;
    const std::size_t m_flatDepth;
    const std::size_t m_diskDepth;

    const std::size_t m_baseOffset;
    const std::size_t m_flatOffset;
    const std::size_t m_diskOffset;

    std::unique_ptr<BaseBranch> m_baseBranch;
};

} // namespace entwine


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
class DiskBranch;
class FlatBranch;
class PointInfo;
class Roller;
class Schema;

// Maintains mapping to house the data belonging to each virtual node.
class Registry
{
public:
    Registry(
            const Schema& schema,
            std::size_t dimensions,
            std::size_t baseDepth,
            std::size_t flatDepth,
            std::size_t diskDepth,
            bool elastic);
    Registry(
            const std::string& path,
            const Schema& schema,
            std::size_t dimensions,
            const Json::Value& meta);
    ~Registry();

    void addPoint(PointInfo** toAddPtr, Roller& roller);

    void query(
            const Roller& roller,
            std::vector<std::size_t>& results,
            std::size_t depthBegin,
            std::size_t depthEnd);

    void query(
            const Roller& roller,
            std::vector<std::size_t>& results,
            const BBox& queryBBox,
            std::size_t depthBegin,
            std::size_t depthEnd);

    std::vector<char> getPointData(std::size_t index);

    void save(const std::string& path, Json::Value& meta) const;

private:
    Branch* getBranch(std::size_t index) const;

    std::unique_ptr<BaseBranch> m_baseBranch;
    std::unique_ptr<FlatBranch> m_flatBranch;
    std::unique_ptr<DiskBranch> m_diskBranch;
};

} // namespace entwine


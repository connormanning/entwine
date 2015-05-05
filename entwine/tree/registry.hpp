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
class Clipper;
class ColdBranch;
class FlatBranch;
class PointInfo;
class Pool;
class Roller;
class Source;
class Schema;

// Maintains mapping to house the data belonging to each virtual node.
class Registry
{
public:
    Registry(
            Source& buildSource,
            const Schema& schema,
            std::size_t dimensions,
            std::size_t chunkPoints,
            std::size_t baseDepth,
            std::size_t flatDepth,
            std::size_t diskDepth);

    Registry(
            Source& buildSource,
            const Schema& schema,
            std::size_t dimensions,
            std::size_t chunkPoints,
            const Json::Value& meta);

    ~Registry();

    bool addPoint(PointInfo** toAddPtr, Roller& roller, Clipper* clipper);
    void clip(Clipper* clipper, std::size_t index);

    void save(Json::Value& meta) const;

    void finalize(
            Source& output,
            Pool& pool,
            std::vector<std::size_t>& ids,
            std::size_t start,
            std::size_t chunkSize);

private:
    // This call will cause the Branch to wake up any serialized data needed
    // to handle calls for this index.  Branch-specific data for what was
    // woken up will be added to this Clipper, and on destruction, this Clipper
    // will signal to the Branch that it can now release those resources.
    Branch* getBranch(Clipper* clipper, std::size_t index) const;

    std::unique_ptr<BaseBranch> m_baseBranch;
    std::unique_ptr<FlatBranch> m_flatBranch;
    std::unique_ptr<ColdBranch> m_coldBranch;
};

} // namespace entwine


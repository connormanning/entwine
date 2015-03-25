/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/branches/disk.hpp>

#include <entwine/third/json/json.h>
#include <entwine/tree/roller.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/platform.hpp>

namespace entwine
{

namespace
{
    std::size_t calcChunkSize(std::size_t depthBegin, std::size_t dimensions)
    {
        return
            Branch::calcOffset(depthBegin + 1, dimensions) -
            Branch::calcOffset(depthBegin, dimensions);
    }
}

bool Chunk::addPoint(const PointInfo* toAdd)
{
    delete toAdd->point;
    delete toAdd;
    return true;
}

DiskBranch::DiskBranch(
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
    : Branch(schema, dimensions, depthBegin, depthEnd)
    , m_chunks()
    , m_chunkSize(calcChunkSize(depthBegin, dimensions))
{ }

DiskBranch::DiskBranch(
        const std::string& path,
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : Branch(schema, dimensions, meta)
    , m_chunks()
    , m_chunkSize(calcChunkSize(depthBegin(), dimensions))
{ }

DiskBranch::~DiskBranch()
{ }

bool DiskBranch::addPoint(PointInfo** toAddPtr, const Roller& roller)
{
    std::size_t chunkId(getChunkId(roller.pos()));

    // Creates the Chunk if it doesn't exist.
    Chunk& chunk(m_chunks[chunkId].get());

    return chunk.addPoint(*toAddPtr);
}

bool DiskBranch::hasPoint(std::size_t index)
{
    bool has(false);

    if (m_chunks[getChunkId(index)].exists())
    {

    }

    return has;
}

Point DiskBranch::getPoint(std::size_t index)
{
    return Point();
}

std::vector<char> DiskBranch::getPointData(
        std::size_t index,
        const Schema& schema)
{
    return std::vector<char>();
}

std::size_t DiskBranch::getChunkId(std::size_t index) const
{
    assert(index >= indexBegin() && index < indexEnd());

    const std::size_t offset(index - indexBegin());
    return offset / m_chunkSize;
}

void DiskBranch::saveImpl(const std::string& path, Json::Value& meta)
{
    // TODO
    /*
    meta["ids"]
    */
}

} // namespace entwine


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
#include <entwine/types/schema.hpp>

namespace entwine
{

DiskBranch::DiskBranch(
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
    : Branch(schema, dimensions, depthBegin, depthEnd)
{ }

DiskBranch::DiskBranch(
        const std::string& path,
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : Branch(schema, dimensions, meta)
{ }

DiskBranch::~DiskBranch()
{ }

bool DiskBranch::addPoint(PointInfo** toAddPtr, const Roller& roller)
{
    return false;
}

const Point* DiskBranch::getPoint(std::size_t index)
{
    return 0;
}

std::vector<char> DiskBranch::getPointData(
        std::size_t index,
        const Schema& schema)
{
    return std::vector<char>();
}

void DiskBranch::saveImpl(const std::string& path, Json::Value& meta)
{
    // TODO
    /*
    meta["ids"]
    */
}

} // namespace entwine


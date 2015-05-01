/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/branches/flat.hpp>

#include <entwine/third/json/json.h>
#include <entwine/types/point.hpp>
#include <entwine/types/schema.hpp>

namespace entwine
{

FlatBranch::FlatBranch(
        Source& source,
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
    : Branch(source, schema, dimensions, depthBegin, depthEnd)
{ }

FlatBranch::FlatBranch(
        Source& source,
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : Branch(source, schema, dimensions, meta)
{
    // TODO Load from disk.
}

FlatBranch::~FlatBranch()
{ }

bool FlatBranch::addPoint(PointInfo** toAddPtr, const Roller& roller)
{
    return false;
}

bool FlatBranch::hasPoint(std::size_t index)
{
    return false;
}

Point FlatBranch::getPoint(std::size_t index)
{
    return Point();
}

std::vector<char> FlatBranch::getPointData(std::size_t index)
{
    return std::vector<char>();
}

void FlatBranch::saveImpl(Json::Value& meta)
{
    // TODO Write data to disk.

    // TODO Write IDs to meta["ids"].
}

void FlatBranch::finalizeImpl(
        Source& output,
        Pool& pool,
        std::vector<std::size_t>& ids,
        std::size_t start,
        std::size_t chunkSize)
{ }

} // namespace entwine


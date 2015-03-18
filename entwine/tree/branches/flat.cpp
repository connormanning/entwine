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
#include <entwine/types/schema.hpp>

namespace entwine
{

FlatBranch::FlatBranch(
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t depthBegin,
        const std::size_t depthEnd,
        const bool elastic)
    : Branch(schema, dimensions, depthBegin, depthEnd)
    , m_elastic(elastic)
{ }

FlatBranch::FlatBranch(
        const std::string& path,
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : Branch(schema, dimensions, meta)
    , m_elastic(meta["elastic"].asBool())
{
    // TODO Load from disk.
}

FlatBranch::~FlatBranch()
{ }

bool FlatBranch::addPoint(PointInfo** toAddPtr, const Roller& roller)
{
    if (m_elastic) std::cout << m_elastic << std::endl;
    return false;
}

const Point* FlatBranch::getPoint(std::size_t index)
{
    return 0;
}

std::vector<char> FlatBranch::getPointData(
        std::size_t index,
        const Schema& schema)
{
    return std::vector<char>();
}

void FlatBranch::saveImpl(const std::string& path, Json::Value& meta)
{
    meta["elastic"] = m_elastic;

    // TODO Write data to disk.

    // TODO Write IDs to meta["ids"].
}

} // namespace entwine


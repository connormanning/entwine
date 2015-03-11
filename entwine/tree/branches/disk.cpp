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
        const std::size_t depthEnd,
        const bool elastic)
    : Branch(schema, dimensions, depthBegin, depthEnd)
    , m_elastic(elastic)
{ }

DiskBranch::DiskBranch(
        const std::string& path,
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : Branch(schema, dimensions, meta)
    , m_elastic(meta["elastic"].asBool())
{ }

DiskBranch::~DiskBranch()
{ }

bool DiskBranch::putPoint(PointInfo** toAddPtr, const Roller& roller)
{
    if (m_elastic) std::cout << m_elastic << std::endl;
    return false;
}

const Point* DiskBranch::getPoint(std::size_t index) const
{
    return 0;
}

void DiskBranch::saveImpl(const std::string& path, Json::Value& meta) const
{
    meta["elastic"] = m_elastic;

    // TODO
    /*
    meta["ids"]
    */
}

} // namespace entwine


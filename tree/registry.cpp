/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "registry.hpp"

#include "json/json.h"

#include "tree/roller.hpp"
#include "tree/branches/base.hpp"
#include "types/bbox.hpp"
#include "types/schema.hpp"
#include "point-info.hpp"

namespace
{
    const std::size_t dimensions(2);

    std::size_t getOffset(std::size_t depth)
    {
        std::size_t offset(0);

        for (std::size_t i(0); i < depth; ++i)
        {
            offset = (offset << dimensions) + 1;
        }

        return offset;
    }
}

namespace entwine
{

Registry::Registry(
        const Schema& schema,
        const std::size_t baseDepth,
        const std::size_t flatDepth,
        const std::size_t diskDepth)
    : m_baseDepth(baseDepth)
    , m_flatDepth(flatDepth)
    , m_diskDepth(diskDepth)
    , m_baseOffset(getOffset(baseDepth))
    , m_flatOffset(getOffset(flatDepth))
    , m_diskOffset(getOffset(diskDepth))
    , m_baseBranch(new BaseBranch(schema, 0, m_baseOffset))
{
    if (baseDepth > flatDepth || flatDepth > diskDepth)
    {
        throw std::runtime_error("Invalid registry params");
    }
}

Registry::~Registry()
{ }

void Registry::put(
        PointInfo** toAddPtr,
        Roller& roller)
{
    PointInfo* toAdd(*toAddPtr);

    Branch* branch(getBranch(roller));

    if (branch)
    {
        if (!branch->putPoint(toAddPtr, roller))
        {
            roller.magnify(toAdd->point);
            put(&toAdd, roller);
        }
    }
    else
    {
        delete toAdd->point;
        delete toAdd;
    }
}

void Registry::getPoints(
        const Roller& roller,
        MultiResults& results,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    const Branch* branch(getBranch(roller));
    if (branch)
    {
        const uint64_t index(roller.pos());
        const Point* point(branch->getPoint(index));

        if (point)
        {
            if (
                    (roller.depth() >= depthBegin) &&
                    (roller.depth() < depthEnd || !depthEnd))
            {
                results.push_back(index);
            }

            if (roller.depth() + 1 < depthEnd || !depthEnd)
            {
                getPoints(roller.getNw(), results, depthBegin, depthEnd);
                getPoints(roller.getNe(), results, depthBegin, depthEnd);
                getPoints(roller.getSw(), results, depthBegin, depthEnd);
                getPoints(roller.getSe(), results, depthBegin, depthEnd);
            }
        }
    }
}

void Registry::getPoints(
        const Roller& roller,
        MultiResults& results,
        const BBox& query,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    if (!roller.bbox().overlaps(query)) return;

    const Branch* branch(getBranch(roller));
    if (branch)
    {
        const uint64_t index(roller.pos());
        const Point* point(branch->getPoint(index));

        if (point)
        {
            if (
                    (roller.depth() >= depthBegin) &&
                    (roller.depth() < depthEnd || !depthEnd) &&
                    (query.contains(*point)))
            {
                results.push_back(index);
            }

            if (roller.depth() + 1 < depthEnd || !depthEnd)
            {
                getPoints(roller.getNw(), results, query, depthBegin, depthEnd);
                getPoints(roller.getNe(), results, query, depthBegin, depthEnd);
                getPoints(roller.getSw(), results, query, depthBegin, depthEnd);
                getPoints(roller.getSe(), results, query, depthBegin, depthEnd);
            }
        }
    }
}

void Registry::save(const std::string& dir, Json::Value& meta) const
{
    Json::Value& treeMeta(meta["tree"]);
    treeMeta["baseDepth"] = static_cast<Json::UInt64>(m_baseDepth);
    treeMeta["flatDepth"] = static_cast<Json::UInt64>(m_flatDepth);
    treeMeta["diskDepth"] = static_cast<Json::UInt64>(m_diskDepth);

    m_baseBranch->save(dir, meta["base"]);
}

void Registry::load(const std::string& dir, const Json::Value& meta)
{
    m_baseBranch->load(dir, meta["base"]);
}

Branch* Registry::getBranch(const Roller& roller) const
{
    Branch* branch(0);

    if (roller.depth() < m_baseDepth)
    {
        branch = m_baseBranch.get();
    }
    else if (roller.depth() < m_flatDepth)
    {
        // TODO
        throw std::runtime_error("No FlatBranch yet");
    }
    else if (roller.depth() < m_diskDepth)
    {
        // TODO
        throw std::runtime_error("No DiskBranch yet");
    }

    return branch;
}

} // namespace entwine


/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/registry.hpp>

#include <algorithm>

#include <entwine/third/json/json.h>
#include <entwine/tree/roller.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/tree/branches/base.hpp>
#include <entwine/tree/branches/disk.hpp>
#include <entwine/tree/branches/flat.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/schema.hpp>

namespace entwine
{

Registry::Registry(
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t baseDepth,
        const std::size_t rawFlatDepth,
        const std::size_t rawDiskDepth,
        const bool elastic)
    : m_baseBranch()
    , m_flatBranch()
    , m_diskBranch()
{
    // Ensure ascending order.
    const std::size_t flatDepth(std::max(baseDepth, rawFlatDepth));
    const std::size_t diskDepth(std::max(flatDepth, rawDiskDepth));

    // If elastic is specified, then the highest branch of the tree will be
    // allowed to grow arbitrarily.  Track whether the highest branch has
    // been created yet.
    bool created(false);

    if (diskDepth > flatDepth)
    {
        m_diskBranch.reset(
                new DiskBranch(
                    schema,
                    dimensions,
                    flatDepth,
                    diskDepth,
                    elastic));

        created = true;
    }

    if (flatDepth > baseDepth)
    {
        m_flatBranch.reset(
                new FlatBranch(
                    schema,
                    dimensions,
                    baseDepth,
                    flatDepth,
                    elastic && !created));

        created = true;
    }

    if (baseDepth)
    {
        if (elastic && !created)
        {
            std::cout << "Elastic request ignored." << std::endl;
        }

        m_baseBranch.reset(
                new BaseBranch(
                    schema,
                    dimensions,
                    baseDepth));
    }
}

Registry::Registry(
        const std::string& path,
        const Schema& schema,
        const std::size_t dimensions,
        const Json::Value& meta)
    : m_baseBranch()
    , m_flatBranch()
    , m_diskBranch()
{
    if (meta.isMember("base"))
    {
        m_baseBranch.reset(
                new BaseBranch(
                    path,
                    schema,
                    dimensions,
                    meta["base"]));
    }

    if (meta.isMember("flat"))
    {
        m_flatBranch.reset(
                new FlatBranch(
                    path,
                    schema,
                    dimensions,
                    meta["flat"]));
    }

    if (meta.isMember("disk"))
    {
        m_diskBranch.reset(
                new DiskBranch(
                    path,
                    schema,
                    dimensions,
                    meta["disk"]));
    }
}

Registry::~Registry()
{ }

void Registry::put(PointInfo** toAddPtr, Roller& roller)
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
        std::vector<std::size_t>& results,
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
        std::vector<std::size_t>& results,
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

void Registry::save(const std::string& path, Json::Value& meta) const
{
    if (m_baseBranch) m_baseBranch->save(path, meta["base"]);
    if (m_flatBranch) m_flatBranch->save(path, meta["flat"]);
    if (m_diskBranch) m_diskBranch->save(path, meta["disk"]);
}

Branch* Registry::getBranch(const Roller& roller) const
{
    Branch* branch(0);
    const std::size_t index(roller.pos());

    if (m_baseBranch && m_baseBranch->accepts(index))
    {
        branch = m_baseBranch.get();
    }
    else if (m_flatBranch && m_flatBranch->accepts(index))
    {
        branch = m_flatBranch.get();
    }
    else if (m_diskBranch && m_diskBranch->accepts(index))
    {
        branch = m_diskBranch.get();
    }

    return branch;
}

} // namespace entwine


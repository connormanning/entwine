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

#include <entwine/http/s3.hpp>
#include <entwine/third/json/json.h>
#include <entwine/tree/roller.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/tree/branches/base.hpp>
#include <entwine/tree/branches/clipper.hpp>
#include <entwine/tree/branches/disk.hpp>
#include <entwine/tree/branches/flat.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

Registry::Registry(
        const std::string& path,
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t baseDepth,
        const std::size_t rawFlatDepth,
        const std::size_t rawDiskDepth)
    : m_baseBranch()
    , m_flatBranch()
    , m_diskBranch()
{
    // Ensure ascending order.
    const std::size_t flatDepth(std::max(baseDepth, rawFlatDepth));
    const std::size_t diskDepth(std::max(flatDepth, rawDiskDepth));

    if (diskDepth > flatDepth)
    {
        m_diskBranch.reset(
                new DiskBranch(
                    path,
                    schema,
                    dimensions,
                    flatDepth,
                    diskDepth));
    }

    if (flatDepth > baseDepth)
    {
        m_flatBranch.reset(
                new FlatBranch(
                    schema,
                    dimensions,
                    baseDepth,
                    flatDepth));
    }

    if (baseDepth)
    {
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

bool Registry::addPoint(PointInfo** toAddPtr, Roller& roller, Clipper* clipper)
{
    bool accepted(false);

    if (Branch* branch = getBranch(clipper, roller.pos()))
    {
        if (!branch->addPoint(toAddPtr, roller))
        {
            roller.magnify((*toAddPtr)->point);
            accepted = addPoint(toAddPtr, roller, clipper);
        }
        else
        {
            accepted = true;
        }
    }
    else
    {
        delete (*toAddPtr)->point;
        delete (*toAddPtr);
    }

    return accepted;
}

void Registry::clip(Clipper* clipper, std::size_t index)
{
    if (Branch* branch = getBranch(clipper, index))
    {
        branch->clip(clipper, index);
    }
}

void Registry::query(
        const Roller& roller,
        Clipper* clipper,
        std::vector<std::size_t>& results,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    const uint64_t index(roller.pos());

    if (Branch* branch = getBranch(clipper, index))
    {
        if (branch->hasPoint(index))
        {
            if (
                    (roller.depth() >= depthBegin) &&
                    (roller.depth() < depthEnd || !depthEnd))
            {
                results.push_back(index);
            }

            if (roller.depth() + 1 < depthEnd || !depthEnd)
            {
                query(roller.getNw(), clipper, results, depthBegin, depthEnd);
                query(roller.getNe(), clipper, results, depthBegin, depthEnd);
                query(roller.getSw(), clipper, results, depthBegin, depthEnd);
                query(roller.getSe(), clipper, results, depthBegin, depthEnd);
            }
        }
    }
}

void Registry::query(
        const Roller& roller,
        Clipper* clipper,
        std::vector<std::size_t>& results,
        const BBox& queryBBox,
        const std::size_t depthBegin,
        const std::size_t depthEnd)
{
    if (!roller.bbox().overlaps(queryBBox)) return;

    const uint64_t index(roller.pos());

    if (Branch* branch = getBranch(clipper, index))
    {
        if (branch->hasPoint(index))
        {
            const Point point(branch->getPoint(index));

            if (
                    (roller.depth() >= depthBegin) &&
                    (roller.depth() < depthEnd || !depthEnd) &&
                    (queryBBox.contains(point)))
            {
                results.push_back(index);
            }

            if (roller.depth() + 1 < depthEnd || !depthEnd)
            {
                const auto nw(roller.getNw());
                const auto ne(roller.getNe());
                const auto sw(roller.getSw());
                const auto se(roller.getSe());

                query(nw, clipper, results, queryBBox, depthBegin, depthEnd);
                query(ne, clipper, results, queryBBox, depthBegin, depthEnd);
                query(sw, clipper, results, queryBBox, depthBegin, depthEnd);
                query(se, clipper, results, queryBBox, depthBegin, depthEnd);
            }
        }
    }
}

std::vector<char> Registry::getPointData(
        Clipper* clipper,
        const std::size_t index)
{
    if (Branch* branch = getBranch(clipper, index))
    {
        return branch->getPointData(index);
    }
    else
    {
        return std::vector<char>();
    }
}

void Registry::save(const std::string& path, Json::Value& meta) const
{
    std::cout << "Saving base" << std::endl;
    if (m_baseBranch) m_baseBranch->save(path, meta["base"]);
    std::cout << "Saving flat" << std::endl;
    if (m_flatBranch) m_flatBranch->save(path, meta["flat"]);
    std::cout << "Saving disk" << std::endl;
    if (m_diskBranch) m_diskBranch->save(path, meta["disk"]);
}

void Registry::finalize(
        S3& output,
        Pool& pool,
        std::vector<std::size_t>& ids,
        std::size_t start,
        std::size_t chunk)
{
    if (m_baseBranch) m_baseBranch->finalize(output, pool, ids, start, chunk);
    if (m_flatBranch) m_flatBranch->finalize(output, pool, ids, start, chunk);
    if (m_diskBranch) m_diskBranch->finalize(output, pool, ids, start, chunk);
}

Branch* Registry::getBranch(Clipper* clipper, const std::size_t index) const
{
    Branch* branch(0);

    if (m_baseBranch && m_baseBranch->accepts(clipper, index))
    {
        branch = m_baseBranch.get();
    }
    else if (m_flatBranch && m_flatBranch->accepts(clipper, index))
    {
        branch = m_flatBranch.get();
    }
    else if (m_diskBranch && m_diskBranch->accepts(clipper, index))
    {
        branch = m_diskBranch.get();
    }

    return branch;
}

} // namespace entwine


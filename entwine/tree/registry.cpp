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

#include <entwine/drivers/source.hpp>
#include <entwine/third/json/json.h>
#include <entwine/tree/roller.hpp>
#include <entwine/tree/point-info.hpp>
#include <entwine/tree/branches/base.hpp>
#include <entwine/tree/branches/clipper.hpp>
#include <entwine/tree/branches/cold.hpp>
#include <entwine/tree/branches/flat.hpp>
#include <entwine/types/bbox.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{

Registry::Registry(
        Source& buildSource,
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t chunkPoints,
        const std::size_t baseDepth,
        const std::size_t rawFlatDepth,
        const std::size_t rawColdDepth)
    : m_baseBranch()
    , m_flatBranch()
    , m_coldBranch()
{
    // Ensure ascending order.
    const std::size_t flatDepth(std::max(baseDepth, rawFlatDepth));
    const std::size_t coldDepth(std::max(flatDepth, rawColdDepth));

    if (coldDepth > flatDepth)
    {
        m_coldBranch.reset(
                new ColdBranch(
                    buildSource,
                    schema,
                    dimensions,
                    chunkPoints,
                    flatDepth,
                    coldDepth));
    }

    if (flatDepth > baseDepth)
    {
        throw std::runtime_error("Flat branch unsupported");
    }

    if (baseDepth)
    {
        m_baseBranch.reset(
                new BaseBranch(
                    buildSource,
                    schema,
                    dimensions,
                    baseDepth));
    }
}

Registry::Registry(
        Source& buildSource,
        const Schema& schema,
        const std::size_t dimensions,
        const std::size_t chunkPoints,
        const Json::Value& meta)
    : m_baseBranch()
    , m_flatBranch()
    , m_coldBranch()
{
    if (meta.isMember("base"))
    {
        m_baseBranch.reset(
                new BaseBranch(
                    buildSource,
                    schema,
                    dimensions,
                    meta["base"]));
    }

    if (meta.isMember("flat"))
    {
        throw std::runtime_error("Flat branch unsupported");
    }

    if (meta.isMember("cold"))
    {
        m_coldBranch.reset(
                new ColdBranch(
                    buildSource,
                    schema,
                    dimensions,
                    chunkPoints,
                    meta["cold"]));
    }
}

Registry::~Registry()
{ }

bool Registry::addPoint(PointInfo** toAddPtr, Roller& roller, Clipper* clipper)
{
    bool accepted(false);

    if (Branch* branch = getBranch(clipper, roller.index()))
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

void Registry::save(Json::Value& meta) const
{
    std::cout << "Saving base" << std::endl;
    if (m_baseBranch) m_baseBranch->save(meta["base"]);
    std::cout << "Saving flat" << std::endl;
    if (m_flatBranch) m_flatBranch->save(meta["flat"]);
    std::cout << "Saving cold" << std::endl;
    if (m_coldBranch) m_coldBranch->save(meta["cold"]);
}

void Registry::finalize(
        Source& output,
        Pool& pool,
        std::vector<std::size_t>& ids,
        std::size_t start,
        std::size_t chunk)
{
    if (m_baseBranch) m_baseBranch->finalize(output, pool, ids, start, chunk);
    if (m_flatBranch) m_flatBranch->finalize(output, pool, ids, start, chunk);
    if (m_coldBranch) m_coldBranch->finalize(output, pool, ids, start, chunk);
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
    else if (m_coldBranch && m_coldBranch->accepts(clipper, index))
    {
        branch = m_coldBranch.get();
    }

    return branch;
}

} // namespace entwine

